package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.PartitionAssigner;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Tags({"redis", "migration", "scan", "source"})
@CapabilityDescription("Scans the source Redis keyspace, partitioned across concurrent tasks by hash-slot "
        + "range (cluster mode) or CRC16 modulo (standalone/sentinel), emitting one empty FlowFile per key "
        + "with type/TTL metadata attributes. Routes by data type for downstream processing by "
        + "RedisTypeDeserializer. Spec section 4.1.")
public class RedisScanReader extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .required(true)
            .identifiesControllerService(RedisConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor MIGRATION_ID = new PropertyDescriptor.Builder()
            .name("migration-id")
            .displayName("Migration ID")
            .description("Unique identifier for this migration run, shared with the other migration "
                    + "processors, used to namespace cursor-checkpoint and partition-claim cache keys.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CURSOR_STATE_CACHE = new PropertyDescriptor.Builder()
            .name("cursor-state-cache")
            .displayName("Cursor State Cache")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor PARTITION_COUNT = new PropertyDescriptor.Builder()
            .name("partition-count")
            .displayName("Partition Count")
            .description("Total number of partitions; must match this processor's Concurrent Tasks setting.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor SCAN_COUNT = new PropertyDescriptor.Builder()
            .name("scan-count")
            .displayName("Scan Count")
            .description("COUNT hint per SCAN call.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("200")
            .build();

    public static final PropertyDescriptor KEY_PATTERN = new PropertyDescriptor.Builder()
            .name("key-pattern")
            .displayName("Key Pattern")
            .description("SCAN MATCH pattern.")
            .required(true)
            .defaultValue("*")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_TYPE_FILTER = new PropertyDescriptor.Builder()
            .name("key-type-filter")
            .displayName("Key Type Filter")
            .description("Comma-separated list of types to include: string,hash,list,set,zset,stream.")
            .required(true)
            .defaultValue("string,hash,list,set,zset,stream")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PREFIX_DENY_LIST = new PropertyDescriptor.Builder()
            .name("prefix-deny-list")
            .displayName("Prefix Deny List")
            .description("Comma-separated list of key prefixes to exclude from migration. A key is "
                    + "skipped if it starts with any of these prefixes. Leave unset to disable.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PREFIX_ONLY_LIST = new PropertyDescriptor.Builder()
            .name("prefix-only-list")
            .displayName("Prefix Only List")
            .description("Comma-separated list of key prefixes to exclusively migrate. If set, a key is "
                    + "skipped unless it starts with one of these prefixes. Applied after Prefix Deny "
                    + "List. Leave unset to migrate all prefixes.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor EMIT_TTL = new PropertyDescriptor.Builder()
            .name("emit-ttl")
            .displayName("Emit TTL")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor SKIP_VOLATILE_KEYS = new PropertyDescriptor.Builder()
            .name("skip-volatile-keys")
            .displayName("Skip Volatile Keys")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor VOLATILE_KEY_TTL_THRESHOLD_MS = new PropertyDescriptor.Builder()
            .name("volatile-key-ttl-threshold-ms")
            .displayName("Volatile Key TTL Threshold (ms)")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor DATABASE_INDEX = new PropertyDescriptor.Builder()
            .name("database-index-override")
            .displayName("Database Index")
            .description("Override the connection pool's default database index. Standalone/sentinel only.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_STRING = new Relationship.Builder().name("string").description("Key is of type STRING").build();
    public static final Relationship REL_HASH = new Relationship.Builder().name("hash").description("Key is of type HASH").build();
    public static final Relationship REL_LIST = new Relationship.Builder().name("list").description("Key is of type LIST").build();
    public static final Relationship REL_SET = new Relationship.Builder().name("set").description("Key is of type SET").build();
    public static final Relationship REL_ZSET = new Relationship.Builder().name("zset").description("Key is of type ZSET").build();
    public static final Relationship REL_STREAM = new Relationship.Builder().name("stream").description("Key is of type STREAM").build();
    public static final Relationship REL_UNKNOWN = new Relationship.Builder().name("unknown").description("Key type is unrecognised or a module type").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("SCAN or TYPE command failed for this key").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REDIS_CONNECTION_POOL, MIGRATION_ID, CURSOR_STATE_CACHE, PARTITION_COUNT, SCAN_COUNT, KEY_PATTERN,
            KEY_TYPE_FILTER, PREFIX_DENY_LIST, PREFIX_ONLY_LIST, EMIT_TTL, SKIP_VOLATILE_KEYS,
            VOLATILE_KEY_TTL_THRESHOLD_MS, DATABASE_INDEX);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_STRING, REL_HASH, REL_LIST, REL_SET, REL_ZSET, REL_STREAM, REL_UNKNOWN, REL_FAILURE);

    private final ThreadLocal<Integer> assignedPartition = new ThreadLocal<>();

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
        RedisConnectionPoolService redisPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        DistributedMapCacheClient cache = context.getProperty(CURSOR_STATE_CACHE).asControllerService(DistributedMapCacheClient.class);
        String migrationId = context.getProperty(MIGRATION_ID).getValue();
        PartitionAssigner assigner = new PartitionAssigner(cache, migrationId);
        int partitionCount = context.getProperty(PARTITION_COUNT).asInteger();

        int partitionIndex = getOrClaimPartition(assigner, partitionCount);
        if (partitionIndex < 0) {
            getLogger().info("No partition available to claim for migration '{}' ({} partitions); yielding", migrationId, partitionCount);
            context.yield();
            return;
        }
        try {
            if (assigner.isPartitionComplete(partitionIndex)) {
                getLogger().info("Partition {} already marked complete for migration '{}'; yielding", partitionIndex, migrationId);
                context.yield();
                return;
            }
        } catch (IOException e) {
            throw new ProcessException("Unable to read partition completion state", e);
        }
        getLogger().info("Scanning partition {} of {} for migration '{}'", partitionIndex, partitionCount, migrationId);

        int scanCount = context.getProperty(SCAN_COUNT).asInteger();
        String pattern = context.getProperty(KEY_PATTERN).getValue();
        Set<String> allowedTypes = Arrays.stream(context.getProperty(KEY_TYPE_FILTER).getValue().split(","))
                .map(String::trim).map(String::toLowerCase).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
        List<byte[]> denyPrefixes = parsePrefixList(context.getProperty(PREFIX_DENY_LIST).getValue());
        List<byte[]> onlyPrefixes = parsePrefixList(context.getProperty(PREFIX_ONLY_LIST).getValue());
        boolean emitTtl = context.getProperty(EMIT_TTL).asBoolean();
        boolean skipVolatile = context.getProperty(SKIP_VOLATILE_KEYS).asBoolean();
        long volatileThresholdMs = context.getProperty(VOLATILE_KEY_TTL_THRESHOLD_MS).isSet()
                ? context.getProperty(VOLATILE_KEY_TTL_THRESHOLD_MS).asLong() : Long.MIN_VALUE;
        Integer databaseIndex = context.getProperty(DATABASE_INDEX).isSet() ? context.getProperty(DATABASE_INDEX).asInteger() : null;
        boolean clusterMode = redisPool.isClusterMode();

        // Lettuce's cluster-mode SCAN rejects any cursor it didn't itself just return - even
        // one rebuilt via ScanCursor.of() from the *same* cursor string ScanCursor.INITIAL
        // would encode - with "A scan in Redis Cluster mode requires to reuse the resulting
        // cursor from the previous scan invocation" (it tracks internal per-node iteration
        // state that a plain string can't carry). Restoring a checkpointed cursor string
        // across a NiFi restart is therefore only possible in STANDALONE/SENTINEL mode; a
        // CLUSTER-mode partition always restarts its scan from ScanCursor.INITIAL, which just
        // re-does some already-seen keys (discarded again by the slot-range filter below), not
        // a correctness issue.
        ScanCursor scanCursor;
        if (clusterMode) {
            scanCursor = ScanCursor.INITIAL;
            getLogger().info("Starting fresh cluster-mode SCAN for partition {} (clusterMode={}, allowedTypes={}, pattern='{}') - "
                    + "cluster SCAN cursors can't be checkpointed/restored across restarts",
                    partitionIndex, clusterMode, allowedTypes, pattern);
        } else {
            String cursor;
            try {
                cursor = assigner.restoreCursor(partitionIndex).orElse(ScanCursor.INITIAL.getCursor());
            } catch (IOException e) {
                throw new ProcessException("Unable to restore SCAN cursor", e);
            }
            getLogger().info("Restored cursor '{}' for partition {} (clusterMode={}, allowedTypes={}, pattern='{}')",
                    cursor, partitionIndex, clusterMode, allowedTypes, pattern);
            scanCursor = ScanCursor.of(cursor);
        }
        while (!Thread.currentThread().isInterrupted()) {
            ScanCursor currentCursor = scanCursor;
            ScanPage page = redisPool.withConnection(cmds ->
                    scanPage(cmds, currentCursor, scanCount, pattern, allowedTypes, denyPrefixes, onlyPrefixes,
                            emitTtl, skipVolatile, volatileThresholdMs, clusterMode, partitionIndex, partitionCount));
            getLogger().info("SCAN page: {} entries, {} failures, cursorAfter='{}', finished={}",
                    page.entries.size(), page.failures.size(), page.cursorAfterPage.getCursor(), page.cursorAfterPage.isFinished());

            for (ScanEntry entry : page.entries) {
                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, entry.attributes(partitionIndex, page.cursorAfterPage, databaseIndex));
                session.transfer(flowFile, relationshipFor(entry.type));
            }
            for (ScanEntry failed : page.failures) {
                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, failed.attributes(partitionIndex, page.cursorAfterPage, databaseIndex));
                session.transfer(flowFile, REL_FAILURE);
            }

            try {
                assigner.checkpointCursor(partitionIndex, page.cursorAfterPage.getCursor());
            } catch (IOException e) {
                throw new ProcessException("Unable to checkpoint SCAN cursor", e);
            }
            session.commit();

            scanCursor = page.cursorAfterPage;
            if (scanCursor.isFinished()) {
                break;
            }
        }

        if (scanCursor.isFinished()) {
            try {
                assigner.markPartitionComplete(partitionIndex);
            } catch (IOException e) {
                getLogger().warn("Failed to mark partition {} complete", partitionIndex, e);
            }
            // Clear so this thread, if NiFi's pool reuses it for a later invocation, claims a
            // fresh partition instead of forever re-returning this now-complete one and yielding.
            assignedPartition.remove();
        }
    }

    private int getOrClaimPartition(PartitionAssigner assigner, int partitionCount) {
        Integer partitionIndex = assignedPartition.get();
        if (partitionIndex != null) {
            return partitionIndex;
        }
        try {
            Optional<Integer> claimed = assigner.claimPartition(partitionCount);
            if (claimed.isEmpty()) {
                return -1;
            }
            assignedPartition.set(claimed.get());
            return claimed.get();
        } catch (IOException e) {
            throw new ProcessException("Unable to claim a scan partition", e);
        }
    }

    private Relationship relationshipFor(String type) {
        return switch (type) {
            case "string" -> REL_STRING;
            case "hash" -> REL_HASH;
            case "list" -> REL_LIST;
            case "set" -> REL_SET;
            case "zset" -> REL_ZSET;
            case "stream" -> REL_STREAM;
            default -> REL_UNKNOWN;
        };
    }

    private static List<byte[]> parsePrefixList(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s.getBytes(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    private static boolean startsWithAny(byte[] key, List<byte[]> prefixes) {
        for (byte[] prefix : prefixes) {
            if (key.length < prefix.length) {
                continue;
            }
            boolean match = true;
            for (int i = 0; i < prefix.length; i++) {
                if (key[i] != prefix[i]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return true;
            }
        }
        return false;
    }

    // LettuceFutures.awaitAll throws if ANY future completed exceptionally, which would
    // otherwise propagate out of scanPage/onTrigger and permanently halt this processor on a
    // single unsupported command (e.g. a source that doesn't implement OBJECT ENCODING) -
    // every subsequent trigger hits the identical, non-transient failure and yields forever.
    // This just waits for completion (success or failure); each future is still resolved and
    // its own failure handled individually right after this call.
    private static void awaitAllTolerantly(RedisFuture<?>[] futures) {
        try {
            LettuceFutures.awaitAll(Duration.ofMinutes(1), futures);
        } catch (Exception ignored) {
            // resolved (and any failure handled) per-future below
        }
    }

    private static ScanPage scanPage(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, ScanCursor cursor, int scanCount, String pattern,
            Set<String> allowedTypes, List<byte[]> denyPrefixes, List<byte[]> onlyPrefixes, boolean emitTtl,
            boolean skipVolatile, long volatileThresholdMs, boolean clusterMode, int partitionIndex,
            int partitionCount) {
        ScanArgs scanArgs = new ScanArgs().match(pattern).limit(scanCount);
        KeyScanCursor<byte[]> result;
        try {
            result = cmds.scan(cursor, scanArgs).get();
        } catch (Exception e) {
            throw new ProcessException("SCAN failed", e);
        }

        List<byte[]> candidateKeys = new ArrayList<>();
        for (byte[] key : result.getKeys()) {
            if (clusterMode) {
                int slot = PartitionAssigner.hashSlot(key);
                int[] range = PartitionAssigner.clusterSlotRange(partitionIndex, partitionCount);
                if (slot < range[0] || slot >= range[1]) {
                    continue;
                }
            } else if (!PartitionAssigner.standaloneOwnsKey(key, partitionIndex, partitionCount)) {
                continue;
            }
            if (!denyPrefixes.isEmpty() && startsWithAny(key, denyPrefixes)) {
                continue;
            }
            if (!onlyPrefixes.isEmpty() && !startsWithAny(key, onlyPrefixes)) {
                continue;
            }
            candidateKeys.add(key);
        }

        List<RedisFuture<String>> typeFutures = new ArrayList<>();
        for (byte[] key : candidateKeys) {
            typeFutures.add(cmds.type(key));
        }
        awaitAllTolerantly(typeFutures.toArray(new RedisFuture<?>[0]));

        List<ScanEntry> entries = new ArrayList<>();
        List<ScanEntry> failures = new ArrayList<>();
        List<byte[]> ttlKeys = new ArrayList<>();
        List<String> ttlTypes = new ArrayList<>();
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>();
        List<RedisFuture<String>> encodingFutures = new ArrayList<>();

        for (int i = 0; i < candidateKeys.size(); i++) {
            byte[] key = candidateKeys.get(i);
            String type;
            try {
                type = typeFutures.get(i).get();
            } catch (Exception e) {
                failures.add(new ScanEntry(key, "unknown", -2, null));
                continue;
            }
            if (!allowedTypes.contains(type.toLowerCase())) {
                continue;
            }
            ttlKeys.add(key);
            ttlTypes.add(type);
            ttlFutures.add(emitTtl ? cmds.pttl(key) : null);
            encodingFutures.add(cmds.objectEncoding(key));
        }
        if (emitTtl) {
            awaitAllTolerantly(ttlFutures.stream().filter(java.util.Objects::nonNull).toArray(RedisFuture<?>[]::new));
        }
        awaitAllTolerantly(encodingFutures.toArray(new RedisFuture<?>[0]));

        for (int i = 0; i < ttlKeys.size(); i++) {
            long ttlMs = -1;
            if (emitTtl) {
                try {
                    ttlMs = ttlFutures.get(i).get();
                } catch (Exception e) {
                    ttlMs = -1;
                }
            }
            if (skipVolatile && ttlMs >= 0 && ttlMs < volatileThresholdMs) {
                continue;
            }
            String encoding = null;
            try {
                encoding = encodingFutures.get(i).get();
            } catch (Exception ignored) {
                // encoding is informational only
            }
            entries.add(new ScanEntry(ttlKeys.get(i), ttlTypes.get(i), ttlMs, encoding));
        }

        return new ScanPage(entries, failures, result);
    }

    private record ScanPage(List<ScanEntry> entries, List<ScanEntry> failures, ScanCursor cursorAfterPage) {
    }

    private record ScanEntry(byte[] key, String type, long ttlMs, String encoding) {
        java.util.Map<String, String> attributes(int partitionIndex, ScanCursor cursor, Integer databaseIndex) {
            java.util.Map<String, String> attrs = new java.util.HashMap<>();
            attrs.put("redis.key", new String(key, StandardCharsets.UTF_8));
            attrs.put("redis.type", type);
            attrs.put("redis.ttl.ms", String.valueOf(ttlMs));
            attrs.put("redis.partition.index", String.valueOf(partitionIndex));
            attrs.put("redis.scan.cursor", cursor.getCursor());
            if (databaseIndex != null) {
                attrs.put("redis.source.db", String.valueOf(databaseIndex));
            }
            if (encoding != null) {
                attrs.put("redis.encoding", encoding);
            }
            return attrs;
        }
    }
}
