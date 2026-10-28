package io.dragonfly.nifi.redis.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dragonfly.nifi.redis.util.ChunkReassembler;
import io.dragonfly.nifi.redis.util.CounterGauge;
import io.dragonfly.nifi.redis.util.FailureReason;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

@Tags({"redis", "migration", "dead-letter", "failure"})
@CapabilityDescription("Terminal sink for failure relationships: writes FlowFile content and attributes "
        + "to a JSON file, emits an ERROR bulletin, and increments the migration.failures.total counter. "
        + "One record per key: a key that fails again replaces its record, which keeps the earlier reasons "
        + "under history. The Dead-Letter Records Pending counter holds the number of records in the output "
        + "directory; moving a reviewed record out of it acknowledges it.")
// Triggered with no input too, to keep the pending count current as records are moved away.
@TriggerWhenEmpty
// One task: replacing a key's record reads the old one for its history first.
@TriggerSerially
public class RedisDeadLetterWriter extends AbstractProcessor {

    public static final PropertyDescriptor OUTPUT_DIRECTORY = new PropertyDescriptor.Builder()
            .name("output-directory")
            .displayName("Output Directory")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Dead-letter file written").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Could not write the dead-letter file").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(OUTPUT_DIRECTORY);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String FAILURE_COUNTER = "migration.failures.total";
    private static final String PENDING_COUNTER = "Dead-Letter Records Pending";
    private static final long RECOUNT_INTERVAL_MS = 10_000;
    private static final int HISTORY_LIMIT = 20;

    private final CounterGauge gauges = new CounterGauge();
    private volatile long countedAtMs;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * Named by key when there is one, so a key that fails again (a reconcile pass retrying a Bloom
     * filter the target can never hold, say) replaces its own record instead of adding another; the
     * record keeps the earlier reasons in its history. A chunk of a large key that failed on its own
     * is named by its index too, so the key's chunks do not replace one another.
     */
    private static String fileName(FlowFile flowFile) {
        String key = flowFile.getAttribute("redis.key");
        if (key == null) {
            return flowFile.getAttribute("uuid") + ".json";
        }
        String chunk = ChunkReassembler.isChunk(flowFile)
                ? "-chunk-" + flowFile.getAttribute(ChunkReassembler.INDEX) + "-of-" + flowFile.getAttribute(ChunkReassembler.TOTAL) : "";
        try {
            // A hash, not the key itself: a key can be longer than a file name may be.
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8));
            return "key-" + HexFormat.of().formatHex(digest) + chunk + ".json";
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The replaced record's reason and the history it already carried, newest first, at most {@link #HISTORY_LIMIT}. */
    @SuppressWarnings("unchecked")
    private static List<Object> history(Path existing) {
        List<Object> history = new ArrayList<>();
        if (!Files.exists(existing)) {
            return history;
        }
        try {
            Map<String, Object> previous = OBJECT_MAPPER.readValue(existing.toFile(), Map.class);
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("reason", previous.get("reason"));
            entry.put("failed_at", previous.get("failed_at"));
            history.add(entry);
            if (previous.get("history") instanceof List<?> older) {
                history.addAll(older);
            }
        } catch (IOException e) {
            // An unreadable earlier record still gets replaced; only its history is lost.
            history.add(Map.of("reason", "an earlier record could not be read: " + e.getMessage()));
        }
        return history.subList(0, Math.min(history.size(), HISTORY_LIMIT));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        Path outputDir = Path.of(context.getProperty(OUTPUT_DIRECTORY).getValue());
        reportPending(session, outputDir);
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            context.yield();
            return;
        }
        try {
            Files.createDirectories(outputDir);
            byte[] content;
            try (InputStream in = session.read(flowFile)) {
                content = in.readAllBytes();
            }
            // An unsupported module key's reason predates the general failure attribute.
            String reason = Objects.requireNonNullElse(flowFile.getAttribute(FailureReason.ATTRIBUTE),
                    Objects.requireNonNullElse(flowFile.getAttribute("redis.incompatible.reason"), "no reason recorded"));
            Path file = outputDir.resolve(fileName(flowFile));
            boolean added = !Files.exists(file);
            Map<String, Object> record = new LinkedHashMap<>();
            record.put("reason", reason);
            record.put("failed_at", Instant.now().toString());
            record.put("history", history(file));
            record.put("attributes", flowFile.getAttributes());
            record.put("content_base64", Base64.getEncoder().encodeToString(content));

            // Written whole, then moved into place: a reader never sees a half-written record. The
            // temporary name does not end in .json, so the pending count never counts it.
            Path written = Files.createTempFile(outputDir, ".writing-", ".tmp");
            try {
                Files.write(written, OBJECT_MAPPER.writeValueAsBytes(record));
                Files.move(written, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } finally {
                Files.deleteIfExists(written);
            }
            // Counted before the FlowFile leaves the queue, so a drained flow never shows a stale count.
            if (added) {
                gauges.add(session, PENDING_COUNTER, 1);
            }

            getLogger().error("Migration failure dead-lettered: key={} reason={} file={}",
                    flowFile.getAttribute("redis.key"), reason, file);
            session.adjustCounter(FAILURE_COUNTER, 1, true);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to write dead-letter file for FlowFile {}", flowFile.getAttribute("uuid"), e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }
    }

    private void reportPending(ProcessSession session, Path outputDir) {
        long now = System.currentTimeMillis();
        if (now - countedAtMs < RECOUNT_INTERVAL_MS) {
            return;
        }
        countedAtMs = now;
        long pending = 0;
        if (Files.isDirectory(outputDir)) {
            try (Stream<Path> files = Files.list(outputDir)) {
                pending = files.filter(f -> f.getFileName().toString().endsWith(".json") && Files.isRegularFile(f)).count();
            } catch (IOException e) {
                getLogger().warn("Could not count the dead-letter records in {}", outputDir, e);
                return;
            }
        }
        gauges.set(session, PENDING_COUNTER, pending);
    }
}
