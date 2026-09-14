package io.dragonfly.nifi.redis.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dragonfly.nifi.redis.services.DragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.KeyRecord;
import io.dragonfly.nifi.redis.util.RedisValueFetcher;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
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

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Tags({"redis", "dragonfly", "migration", "validation"})
@CapabilityDescription("Post-migration spot-check: samples keys from the source, compares them against "
        + "the target, and emits a validation report FlowFile. Spec section 4.6.")
public class MigrationValidationProcessor extends AbstractProcessor {

    public enum ValidationMode { EXISTENCE, TYPE, FULL_VALUE, TTL_DELTA }

    public enum ReportFormat { JSON, CSV }

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

    public static final PropertyDescriptor VALIDATION_MODE = new PropertyDescriptor.Builder()
            .name("validation-mode")
            .displayName("Validation Mode")
            .required(true)
            .allowableValues(ValidationMode.EXISTENCE.name(), ValidationMode.TYPE.name(), ValidationMode.FULL_VALUE.name(), ValidationMode.TTL_DELTA.name())
            .defaultValue(ValidationMode.FULL_VALUE.name())
            .build();

    public static final PropertyDescriptor SAMPLE_SIZE = new PropertyDescriptor.Builder()
            .name("sample-size")
            .displayName("Sample Size")
            .description("Number of keys to validate per trigger, taken from one SCAN page on the source "
                    + "(a spot-check sample, not a uniformly random sample of the full keyspace).")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor TTL_DELTA_TOLERANCE_MS = new PropertyDescriptor.Builder()
            .name("ttl-delta-tolerance-ms")
            .displayName("TTL Delta Tolerance (ms)")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("5000")
            .build();

    public static final PropertyDescriptor REPORT_FORMAT = new PropertyDescriptor.Builder()
            .name("report-format")
            .displayName("Report Format")
            .required(true)
            .allowableValues(ReportFormat.JSON.name(), ReportFormat.CSV.name())
            .defaultValue(ReportFormat.JSON.name())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Validation report emitted").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Validation run itself failed").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SOURCE_CONNECTION_POOL, TARGET_CONNECTION_POOL, VALIDATION_MODE, SAMPLE_SIZE, TTL_DELTA_TOLERANCE_MS, REPORT_FORMAT);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private record KeyCheck(String key, boolean missingOnTarget, boolean typeMismatch, boolean valueMismatch, boolean passed) {
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        RedisConnectionPoolService sourcePool = context.getProperty(SOURCE_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        DragonflyConnectionPoolService targetPool = context.getProperty(TARGET_CONNECTION_POOL).asControllerService(DragonflyConnectionPoolService.class);
        ValidationMode mode = ValidationMode.valueOf(context.getProperty(VALIDATION_MODE).getValue());
        int sampleSize = context.getProperty(SAMPLE_SIZE).asInteger();
        long ttlToleranceMs = context.getProperty(TTL_DELTA_TOLERANCE_MS).asLong();
        ReportFormat reportFormat = ReportFormat.valueOf(context.getProperty(REPORT_FORMAT).getValue());

        List<byte[]> sampledKeys;
        try {
            sampledKeys = sourcePool.withConnection(cmds -> {
                try {
                    return cmds.scan(ScanCursor.INITIAL, new ScanArgs().limit(sampleSize)).get().getKeys();
                } catch (Exception e) {
                    throw new ProcessException("Sample SCAN failed", e);
                }
            });
        } catch (Exception e) {
            getLogger().error("Validation sampling failed", e);
            FlowFile failure = session.create();
            session.transfer(failure, REL_FAILURE);
            return;
        }

        List<KeyCheck> checks = new ArrayList<>();
        for (byte[] keyBytes : sampledKeys) {
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            try {
                checks.add(checkKey(sourcePool, targetPool, key, keyBytes, mode, ttlToleranceMs));
            } catch (Exception e) {
                getLogger().warn("Validation check failed for key {}", key, e);
                checks.add(new KeyCheck(key, false, false, true, false));
            }
        }

        FlowFile report = session.create();
        report = session.putAllAttributes(report, reportAttributes(checks));
        report = session.write(report, out -> writeReport(out, checks, reportFormat));
        session.transfer(report, REL_SUCCESS);
    }

    private KeyCheck checkKey(RedisConnectionPoolService sourcePool, DragonflyConnectionPoolService targetPool,
                               String key, byte[] keyBytes, ValidationMode mode, long ttlToleranceMs) throws Exception {
        boolean existsOnTarget = targetPool.withConnection(cmds -> get(cmds.exists(keyBytes))) > 0;
        if (!existsOnTarget) {
            return new KeyCheck(key, true, false, false, false);
        }
        if (mode == ValidationMode.EXISTENCE) {
            return new KeyCheck(key, false, false, false, true);
        }

        String sourceType = sourcePool.withConnection(cmds -> get(cmds.type(keyBytes)));
        String targetType = targetPool.withConnection(cmds -> get(cmds.type(keyBytes)));
        boolean typeMismatch = !sourceType.equals(targetType);
        if (mode == ValidationMode.TYPE) {
            return new KeyCheck(key, false, typeMismatch, false, !typeMismatch);
        }
        if (typeMismatch) {
            return new KeyCheck(key, false, true, false, false);
        }

        if (mode == ValidationMode.TTL_DELTA) {
            long sourceTtl = sourcePool.withConnection(cmds -> get(cmds.pttl(keyBytes)));
            long targetTtl = targetPool.withConnection(cmds -> get(cmds.pttl(keyBytes)));
            boolean withinTolerance = Math.abs(sourceTtl - targetTtl) <= ttlToleranceMs;
            return new KeyCheck(key, false, false, !withinTolerance, withinTolerance);
        }

        // FULL_VALUE
        Optional<List<KeyRecord>> sourceValue = sourcePool.withConnection(cmds ->
                RedisValueFetcher.fetch(cmds, key, sourceType, -1, null, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, false));
        Optional<List<KeyRecord>> targetValue = targetPool.withConnection(cmds ->
                RedisValueFetcher.fetch(cmds, key, targetType, -1, null, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, false));
        boolean valueMismatch = !valuesEqual(sourceType, sourceValue, targetValue);
        return new KeyCheck(key, false, false, valueMismatch, !valueMismatch);
    }

    private static boolean valuesEqual(String type, Optional<List<KeyRecord>> a, Optional<List<KeyRecord>> b) {
        if (a.isEmpty() || b.isEmpty()) {
            return a.isEmpty() == b.isEmpty();
        }
        Object valueA = a.get().get(0).value;
        Object valueB = b.get().get(0).value;
        return switch (type) {
            case "set" -> new HashSet<>((List<?>) valueA).equals(new HashSet<>((List<?>) valueB));
            case "zset" -> canonicalZsetEntries((List<?>) valueA).equals(canonicalZsetEntries((List<?>) valueB));
            default -> java.util.Objects.equals(valueA, valueB);
        };
    }

    @SuppressWarnings("unchecked")
    private static Set<String> canonicalZsetEntries(List<?> entries) {
        Set<String> canonical = new HashSet<>();
        for (Object o : entries) {
            Map<String, Object> entry = (Map<String, Object>) o;
            canonical.add(entry.get("member") + ":" + entry.get("score"));
        }
        return canonical;
    }

    private static Map<String, String> reportAttributes(List<KeyCheck> checks) {
        long passed = 0;
        long missingOnTarget = 0;
        long typeMismatch = 0;
        long valueMismatch = 0;
        for (KeyCheck check : checks) {
            if (check.passed()) passed++;
            if (check.missingOnTarget()) missingOnTarget++;
            if (check.typeMismatch()) typeMismatch++;
            if (check.valueMismatch()) valueMismatch++;
        }
        Map<String, String> attrs = new HashMap<>();
        attrs.put("validation.total_checked", String.valueOf(checks.size()));
        attrs.put("validation.passed", String.valueOf(passed));
        attrs.put("validation.failed", String.valueOf(checks.size() - passed));
        attrs.put("validation.missing_on_target", String.valueOf(missingOnTarget));
        attrs.put("validation.type_mismatch", String.valueOf(typeMismatch));
        attrs.put("validation.value_mismatch", String.valueOf(valueMismatch));
        return attrs;
    }

    private static void writeReport(OutputStream out, List<KeyCheck> checks, ReportFormat format) throws java.io.IOException {
        if (format == ReportFormat.CSV) {
            out.write("key,missing_on_target,type_mismatch,value_mismatch,passed\n".getBytes(StandardCharsets.UTF_8));
            for (KeyCheck check : checks) {
                String line = String.join(",", check.key(), String.valueOf(check.missingOnTarget()),
                        String.valueOf(check.typeMismatch()), String.valueOf(check.valueMismatch()), String.valueOf(check.passed())) + "\n";
                out.write(line.getBytes(StandardCharsets.UTF_8));
            }
        } else {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (KeyCheck check : checks) {
                Map<String, Object> row = new HashMap<>();
                row.put("key", check.key());
                row.put("missing_on_target", check.missingOnTarget());
                row.put("type_mismatch", check.typeMismatch());
                row.put("value_mismatch", check.valueMismatch());
                row.put("passed", check.passed());
                rows.add(row);
            }
            OBJECT_MAPPER.writeValue(out, rows);
        }
    }

    private static <T> T get(java.util.concurrent.Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new ProcessException(e);
        }
    }
}
