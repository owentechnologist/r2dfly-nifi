package io.dragonfly.nifi.redis.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dragonfly.nifi.redis.services.DragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.ComparableValue;
import io.dragonfly.nifi.redis.util.KeyFilter;
import io.dragonfly.nifi.redis.util.KeyRecord;
import io.dragonfly.nifi.redis.util.RedisValueFetcher;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Finds keys that differ between source and target and sends each one to repair, and reports what
 * it found. A repair is the key's name on {@code repair}, connected to the same
 * RedisTypeDeserializer (live) the replication reader feeds: that copies the key's current value,
 * or deletes it from the target when the source no longer has it, in the same per-key order as
 * every other change. The reconciler itself never writes to either side.
 *
 * <p>A pass walks the source (keys missing or different on the target) and then the target (keys
 * the source no longer has), one SCAN page at a time within a time budget per trigger, so a large
 * keyspace never holds a NiFi thread for long. It catches what no change stream sees: an event lost
 * while a consumer was down, a FLUSHALL, a write made directly to the target.
 */
@Tags({"redis", "dragonfly", "migration", "reconcile", "verify", "repair"})
@CapabilityDescription("Compares source and target key by key (existence, type, size, TTL, and value), routes every key "
        + "that differs to repair for re-copying, and emits a JSON or Markdown report per pass. A sudden drop in the "
        + "source's DBSIZE (a FLUSHALL or FLUSHDB) starts a full pass at once. Assumes target keys carry the source's "
        + "names (no Key Prefix on the writer).")
@PrimaryNodeOnly
@TriggerSerially
// No input: NiFi refuses Primary Node Only for a processor with an incoming connection, and a
// reconciler on every node of a NiFi cluster would scan both databases once per node.
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class MigrationReconciler extends AbstractProcessor {

    public enum Mode { FULL, SAMPLE }

    public enum ReportFormat { JSON, MARKDOWN }

    public static final PropertyDescriptor SOURCE_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("source-connection-pool")
            .displayName("Source Connection Pool")
            .required(true)
            .identifiesControllerService(RedisConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor TARGET_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("target-connection-pool")
            .displayName("Target Connection Pool")
            .required(true)
            .identifiesControllerService(DragonflyConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("mode")
            .displayName("Mode")
            .description("FULL compares every key on both sides. SAMPLE compares Sample Size random keys from each side, "
                    + "a cheap standing check for a large keyspace. A pass started by a DBSIZE drop is always FULL.")
            .required(true)
            .allowableValues(Mode.FULL.name(), Mode.SAMPLE.name())
            .defaultValue(Mode.SAMPLE.name())
            .build();

    public static final PropertyDescriptor PASS_INTERVAL_MS = new PropertyDescriptor.Builder()
            .name("pass-interval-ms")
            .displayName("Pass Interval (ms)")
            .description("Time from the end of one pass to the start of the next, unless a full pass is requested earlier.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("600000")
            .build();

    public static final PropertyDescriptor SAMPLE_SIZE = new PropertyDescriptor.Builder()
            .name("sample-size")
            .displayName("Sample Size")
            .description("Random keys drawn from each side per SAMPLE pass.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor SCAN_COUNT = new PropertyDescriptor.Builder()
            .name("scan-count")
            .displayName("Scan Count")
            .description("COUNT hint per SCAN page of a FULL pass.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("500")
            .build();

    public static final PropertyDescriptor TIME_BUDGET_MS = new PropertyDescriptor.Builder()
            .name("time-budget-ms")
            .displayName("Time Budget per Trigger (ms)")
            .description("A FULL pass stops taking new pages once a trigger has run this long, and continues on the next.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("2000")
            .build();

    public static final PropertyDescriptor TTL_TOLERANCE_MS = new PropertyDescriptor.Builder()
            .name("ttl-tolerance-ms")
            .displayName("TTL Tolerance (ms)")
            .description("Remaining TTLs further apart than this count as a difference; so does a TTL on one side only.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("5000")
            .build();

    public static final PropertyDescriptor COMPARE_VALUES = new PropertyDescriptor.Builder()
            .name("compare-values")
            .displayName("Compare Values")
            .description("Read and compare the full value of core-type keys whose type, size and TTL already match. "
                    + "Off, the check stops at size, which misses a value changed in place. Module types are compared "
                    + "by type only.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor DBSIZE_DROP_PERCENT = new PropertyDescriptor.Builder()
            .name("dbsize-drop-percent")
            .displayName("DBSIZE Drop Percent")
            .description("A full pass starts when the source's DBSIZE falls by at least this percent between two "
                    + "triggers (FLUSHALL, FLUSHDB or a mass delete, which a keyspace-notification Live Phase never "
                    + "sees as such). 0 turns the check off.")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(0, 100, true))
            .defaultValue("50")
            .build();

    public static final PropertyDescriptor REPORT_FORMAT = new PropertyDescriptor.Builder()
            .name("report-format")
            .displayName("Report Format")
            .required(true)
            .allowableValues(ReportFormat.JSON.name(), ReportFormat.MARKDOWN.name())
            .defaultValue(ReportFormat.JSON.name())
            .build();

    public static final Relationship REL_REPAIR = new Relationship.Builder().name("repair")
            .description("A key that differs (redis.key, redis.reconcile.reason); connect to RedisTypeDeserializer (live)").build();
    public static final Relationship REL_REPORT = new Relationship.Builder().name("report")
            .description("One report per finished pass").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SOURCE_CONNECTION_POOL, TARGET_CONNECTION_POOL, MODE, PASS_INTERVAL_MS, SAMPLE_SIZE, SCAN_COUNT,
            TIME_BUDGET_MS, TTL_TOLERANCE_MS, COMPARE_VALUES, RedisScanReader.PREFIX_DENY_LIST,
            RedisScanReader.PREFIX_ONLY_LIST, DBSIZE_DROP_PERCENT, REPORT_FORMAT);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_REPAIR, REL_REPORT);

    private static final Set<String> CORE_TYPES = Set.of("string", "hash", "list", "set", "zset", "stream");
    private static final int REPORTED_DIFFERENCES = 100;
    private static final Duration AWAIT = Duration.ofSeconds(30);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    record Difference(String key, String reason) {
    }

    /** One pass in progress. A null cursor is a side not started; a finished cursor is a side done. */
    private static final class Pass {
        final Mode mode;
        final String trigger;
        final Instant startedAt = Instant.now();
        ScanCursor sourceCursor;
        ScanCursor targetCursor;
        long keysChecked;
        final Map<String, Long> counts = new TreeMap<>();
        final List<Difference> examples = new ArrayList<>();

        Pass(Mode mode, String trigger) {
            this.mode = mode;
            this.trigger = trigger;
        }

        void record(Difference difference) {
            counts.merge(difference.reason(), 1L, Long::sum);
            if (examples.size() < REPORTED_DIFFERENCES) {
                examples.add(difference);
            }
        }

        long differences() {
            return counts.values().stream().mapToLong(Long::longValue).sum();
        }

        /** A page that fails is retried whole; what it recorded before failing must not count twice. */
        Runnable undoPoint() {
            long checked = keysChecked;
            Map<String, Long> countsBefore = new TreeMap<>(counts);
            int examplesBefore = examples.size();
            return () -> {
                keysChecked = checked;
                counts.clear();
                counts.putAll(countsBefore);
                examples.subList(examplesBefore, examples.size()).clear();
            };
        }
    }

    private volatile Pass pass;
    private volatile String fullPassRequestedBy;
    private volatile long lastPassEndedAtMs;
    private volatile long lastDbsize = -1;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /** The first pass starts at once, so a newly started flow reports promptly. */
    @OnScheduled
    public void onScheduled() {
        pass = null;
        fullPassRequestedBy = null;
        lastPassEndedAtMs = 0;
        lastDbsize = -1;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        RedisConnectionPoolService source = context.getProperty(SOURCE_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        RedisConnectionPoolService target = context.getProperty(TARGET_CONNECTION_POOL).asControllerService(DragonflyConnectionPoolService.class);
        KeyFilter filter = KeyFilter.parse(null, context.getProperty(RedisScanReader.PREFIX_DENY_LIST).getValue(),
                context.getProperty(RedisScanReader.PREFIX_ONLY_LIST).getValue());

        watchDbsize(context, source);

        Pass current = pass;
        if (current == null || fullPassRequestedBy != null && current.mode != Mode.FULL) {
            current = startPassIfDue(context);
            if (current == null) {
                session.commit();
                context.yield();
                return;
            }
        }

        if (current.mode == Mode.SAMPLE) {
            int size = context.getProperty(SAMPLE_SIZE).asInteger();
            compareSourceKeys(context, session, source, target, randomKeys(source, size, filter), current);
            findKeysOnlyOnTarget(session, source, randomKeys(target, size, filter), current);
            finish(context, session, current);
            return;
        }

        long deadline = System.currentTimeMillis() + context.getProperty(TIME_BUDGET_MS).asLong();
        int scanCount = context.getProperty(SCAN_COUNT).asInteger();
        while (System.currentTimeMillis() < deadline) {
            Runnable undo = current.undoPoint();
            try {
                if (!step(context, session, source, target, filter, scanCount, current)) {
                    return;
                }
            } catch (RuntimeException e) {
                undo.run();
                throw e;
            }
            // Repairs reach the copy flow page by page instead of all at the end of a long pass.
            session.commit();
        }
    }

    /** One page of a FULL pass; false once the pass has finished and reported. */
    private boolean step(ProcessContext context, ProcessSession session, RedisConnectionPoolService source,
                         RedisConnectionPoolService target, KeyFilter filter, int scanCount, Pass current) {
        if (current.sourceCursor == null || !current.sourceCursor.isFinished()) {
            KeyScanCursor<byte[]> page = scan(source, current.sourceCursor, scanCount);
            compareSourceKeys(context, session, source, target, allowed(page.getKeys(), filter), current);
            // Only after the page is compared: a failure above retries this page next trigger
            // rather than leaving it out of a pass that would then report clean.
            current.sourceCursor = page;
        } else if (current.targetCursor == null || !current.targetCursor.isFinished()) {
            KeyScanCursor<byte[]> page = scan(target, current.targetCursor, scanCount);
            findKeysOnlyOnTarget(session, source, allowed(page.getKeys(), filter), current);
            current.targetCursor = page;
        } else {
            finish(context, session, current);
            return false;
        }
        return true;
    }

    private Pass startPassIfDue(ProcessContext context) {
        String requestedBy = fullPassRequestedBy;
        Pass started;
        if (requestedBy != null) {
            started = new Pass(Mode.FULL, requestedBy);
            fullPassRequestedBy = null;
        } else if (System.currentTimeMillis() - lastPassEndedAtMs >= context.getProperty(PASS_INTERVAL_MS).asLong()) {
            started = new Pass(Mode.valueOf(context.getProperty(MODE).getValue()), "schedule");
        } else {
            return null;
        }
        getLogger().info("Reconcile pass starting: {} (triggered by {})", started.mode, started.trigger);
        pass = started;
        return started;
    }

    private void watchDbsize(ProcessContext context, RedisConnectionPoolService source) {
        long dropPercent = context.getProperty(DBSIZE_DROP_PERCENT).asLong();
        if (dropPercent == 0) {
            return;
        }
        long dbsize = await(source, RedisClusterAsyncCommands::dbsize);
        long previous = lastDbsize;
        lastDbsize = dbsize;
        if (previous > 0 && dbsize <= previous * (100 - dropPercent) / 100) {
            getLogger().warn("Source DBSIZE fell from {} to {}; starting a full reconcile pass", previous, dbsize);
            fullPassRequestedBy = "a DBSIZE drop from " + previous + " to " + dbsize;
        }
    }

    /**
     * Keys missing on the target, or different there. Each side's TYPE and PTTL go out in one
     * pipeline; sizes only for keys whose types match, and values only where sizes match too.
     */
    private void compareSourceKeys(ProcessContext context, ProcessSession session, RedisConnectionPoolService source,
                                   RedisConnectionPoolService target, List<byte[]> keys, Pass current) {
        if (keys.isEmpty()) {
            return;
        }
        long ttlTolerance = context.getProperty(TTL_TOLERANCE_MS).asLong();
        boolean compareValues = context.getProperty(COMPARE_VALUES).asBoolean();
        List<String> sourceTypes = pipeline(source, keys, (cmds, key) -> cmds.type(key));
        List<String> targetTypes = pipeline(target, keys, (cmds, key) -> cmds.type(key));
        List<Long> sourceTtls = pipeline(source, keys, (cmds, key) -> cmds.pttl(key));
        List<Long> targetTtls = pipeline(target, keys, (cmds, key) -> cmds.pttl(key));
        List<byte[]> sameType = new ArrayList<>();
        List<String> sameTypeNames = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            String key = new String(keys.get(i), StandardCharsets.UTF_8);
            String sourceType = sourceTypes.get(i);
            String targetType = targetTypes.get(i);
            if ("none".equals(sourceType)) {
                continue;
            }
            current.keysChecked++;
            if ("none".equals(targetType)) {
                repair(session, current, key, "missing_on_target");
            } else if (!sourceType.equals(targetType)) {
                repair(session, current, key, "type");
            } else if (ttlDiffers(sourceTtls.get(i), targetTtls.get(i), ttlTolerance)) {
                repair(session, current, key, "ttl");
            } else if (CORE_TYPES.contains(sourceType)) {
                sameType.add(keys.get(i));
                sameTypeNames.add(sourceType);
            }
        }
        List<Long> sourceSizes = sizes(source, sameType, sameTypeNames);
        List<Long> targetSizes = sizes(target, sameType, sameTypeNames);
        for (int i = 0; i < sameType.size(); i++) {
            String key = new String(sameType.get(i), StandardCharsets.UTF_8);
            if (!sourceSizes.get(i).equals(targetSizes.get(i))) {
                repair(session, current, key, "size");
            } else if (compareValues && !sameValue(source, target, key, sameTypeNames.get(i))) {
                repair(session, current, key, "value");
            }
        }
    }

    /** Keys the target has and the source does not: the repair deletes them from the target. */
    private void findKeysOnlyOnTarget(ProcessSession session, RedisConnectionPoolService source, List<byte[]> keys, Pass current) {
        if (keys.isEmpty()) {
            return;
        }
        List<Long> exists = pipeline(source, keys, (cmds, key) -> cmds.exists(key));
        for (int i = 0; i < keys.size(); i++) {
            if (exists.get(i) == 0) {
                repair(session, current, new String(keys.get(i), StandardCharsets.UTF_8), "extra_on_target");
            }
        }
    }

    private static boolean ttlDiffers(long sourceTtl, long targetTtl, long tolerance) {
        boolean sourceExpires = sourceTtl > 0;
        boolean targetExpires = targetTtl > 0;
        return sourceExpires != targetExpires || (sourceExpires && Math.abs(sourceTtl - targetTtl) > tolerance);
    }

    private List<Long> sizes(RedisConnectionPoolService pool, List<byte[]> keys, List<String> types) {
        List<RedisFuture<Long>> futures = new ArrayList<>(keys.size());
        return pool.withConnection(cmds -> {
            for (int i = 0; i < keys.size(); i++) {
                byte[] key = keys.get(i);
                futures.add(switch (types.get(i)) {
                    case "string" -> cmds.strlen(key);
                    case "hash" -> cmds.hlen(key);
                    case "list" -> cmds.llen(key);
                    case "set" -> cmds.scard(key);
                    case "zset" -> cmds.zcard(key);
                    default -> cmds.xlen(key);
                });
            }
            return awaitAll(futures);
        });
    }

    /** A change between the two reads shows up as a difference; the repair it causes copies the current value, which is harmless. */
    private boolean sameValue(RedisConnectionPoolService source, RedisConnectionPoolService target, String key, String type) {
        Optional<List<KeyRecord>> a = source.withConnection(cmds -> whole(cmds, key, type));
        Optional<List<KeyRecord>> b = target.withConnection(cmds -> whole(cmds, key, type));
        if (a.isEmpty() || b.isEmpty()) {
            return a.isEmpty() && b.isEmpty();
        }
        return ComparableValue.of(a.get().get(0)).equals(ComparableValue.of(b.get().get(0)));
    }

    private static Optional<List<KeyRecord>> whole(RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, String type) {
        return RedisValueFetcher.fetch(cmds, key, type, -1L, null, Integer.MAX_VALUE, Integer.MAX_VALUE, 1000, true);
    }

    private void repair(ProcessSession session, Pass current, String key, String reason) {
        current.record(new Difference(key, reason));
        FlowFile flowFile = session.putAllAttributes(session.create(), Map.of("redis.key", key, "redis.reconcile.reason", reason));
        session.transfer(flowFile, REL_REPAIR);
    }

    private void finish(ProcessContext context, ProcessSession session, Pass finished) {
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("mode", finished.mode.name());
        report.put("triggered_by", finished.trigger);
        report.put("started_at", finished.startedAt.toString());
        report.put("finished_at", Instant.now().toString());
        report.put("keys_checked", finished.keysChecked);
        report.put("differences", finished.differences());
        report.put("differences_by_reason", finished.counts);
        report.put("examples", finished.examples.stream().map(d -> Map.of("key", d.key(), "reason", d.reason())).toList());
        ReportFormat format = ReportFormat.valueOf(context.getProperty(REPORT_FORMAT).getValue());
        byte[] content = format == ReportFormat.JSON ? json(report) : markdown(finished).getBytes(StandardCharsets.UTF_8);

        FlowFile flowFile = session.write(session.create(), out -> out.write(content));
        flowFile = session.putAllAttributes(flowFile, Map.of(
                "reconcile.mode", finished.mode.name(),
                "reconcile.triggered.by", finished.trigger,
                "reconcile.keys.checked", String.valueOf(finished.keysChecked),
                "reconcile.differences", String.valueOf(finished.differences()),
                "mime.type", format == ReportFormat.JSON ? "application/json" : "text/markdown"));
        session.transfer(flowFile, REL_REPORT);
        session.adjustCounter("Reconcile Passes", 1, false);
        session.adjustCounter("Reconcile Keys Checked", finished.keysChecked, false);
        finished.counts.forEach((reason, count) -> session.adjustCounter("Reconcile Differences (" + reason + ")", count, false));
        session.commit();
        getLogger().info("Reconcile pass finished: {} keys checked, {} differences {} sent to repair",
                finished.keysChecked, finished.differences(), finished.counts);
        pass = null;
        lastPassEndedAtMs = System.currentTimeMillis();
    }

    private static byte[] json(Map<String, Object> report) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(report);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new ProcessException(e);
        }
    }

    private static String markdown(Pass finished) {
        StringBuilder md = new StringBuilder();
        md.append("# Reconcile report\n\n");
        md.append("- Mode: ").append(finished.mode).append('\n');
        md.append("- Triggered by: ").append(finished.trigger).append('\n');
        md.append("- Started: ").append(finished.startedAt).append('\n');
        md.append("- Keys checked: ").append(finished.keysChecked).append('\n');
        md.append("- Differences sent to repair: ").append(finished.differences()).append("\n\n");
        if (!finished.counts.isEmpty()) {
            md.append("| Reason | Keys |\n|---|---|\n");
            finished.counts.forEach((reason, count) -> md.append("| ").append(reason).append(" | ").append(count).append(" |\n"));
            md.append("\n| Example key | Reason |\n|---|---|\n");
            finished.examples.forEach(d -> md.append("| `").append(d.key().replace("|", "\\|")).append("` | ").append(d.reason()).append(" |\n"));
        }
        return md.toString();
    }

    private static List<byte[]> allowed(List<byte[]> keys, KeyFilter filter) {
        return keys.stream().filter(filter::allowsKey).toList();
    }

    private static List<byte[]> randomKeys(RedisConnectionPoolService pool, int count, KeyFilter filter) {
        List<byte[]> drawn = pool.withConnection(cmds -> {
            List<RedisFuture<byte[]>> futures = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                futures.add(cmds.randomkey());
            }
            return awaitAll(futures);
        });
        Map<String, byte[]> distinct = new LinkedHashMap<>();
        for (byte[] key : drawn) {
            if (key != null && filter.allowsKey(key)) {
                distinct.putIfAbsent(new String(key, StandardCharsets.UTF_8), key);
            }
        }
        return new ArrayList<>(distinct.values());
    }

    private static KeyScanCursor<byte[]> scan(RedisConnectionPoolService pool, ScanCursor cursor, int count) {
        return await(pool, cmds -> cmds.scan(cursor == null ? ScanCursor.INITIAL : cursor, ScanArgs.Builder.limit(count)));
    }

    private interface KeyCommand<T> {
        RedisFuture<T> issue(RedisClusterAsyncCommands<byte[], byte[]> cmds, byte[] key);
    }

    private static <T> List<T> pipeline(RedisConnectionPoolService pool, List<byte[]> keys, KeyCommand<T> command) {
        return pool.withConnection(cmds -> {
            List<RedisFuture<T>> futures = new ArrayList<>(keys.size());
            for (byte[] key : keys) {
                futures.add(command.issue(cmds, key));
            }
            return awaitAll(futures);
        });
    }

    /** Any failure fails the trigger: a comparison with a gap in it would report a clean pass it did not make. */
    private static <T> List<T> awaitAll(List<RedisFuture<T>> futures) {
        if (!LettuceFutures.awaitAll(AWAIT, futures.toArray(new RedisFuture[0]))) {
            throw new ProcessException("Timed out waiting for " + futures.size() + " replies");
        }
        List<T> results = new ArrayList<>(futures.size());
        for (RedisFuture<T> future : futures) {
            try {
                results.add(future.get());
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        }
        return results;
    }

    private static <T> T await(RedisConnectionPoolService pool, Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<T>> call) {
        return pool.withConnection(cmds -> awaitAll(List.of(call.apply(cmds))).get(0));
    }
}
