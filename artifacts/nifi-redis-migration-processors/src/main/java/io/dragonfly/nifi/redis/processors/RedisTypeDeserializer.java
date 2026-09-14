package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.KeyRecord;
import io.dragonfly.nifi.redis.util.RedisTypeSerializer;
import io.dragonfly.nifi.redis.util.RedisValueFetcher;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Reads the full value of a key emitted by RedisScanReader and writes it as a structured JSON
 * payload into the FlowFile content, per spec section 4.2.
 *
 * <p>Pipelines string/hash/list/set/zset reads across a batch of keys per trigger - dispatching
 * every key's read command concurrently on one borrowed connection, awaiting them all together,
 * then resolving and serializing each individually - instead of one blocking round trip per key.
 * This matters most when the source is a remote/high-latency endpoint: verified in practice that
 * throughput was capped at roughly 1/round-trip-latency keys/sec against a real cloud source,
 * since every key previously incurred its own full network round trip with no pipelining at all
 * (the same class of bottleneck already fixed for module types in ModuleTypeHandler and for
 * writes in RedisBatchWriter, just never applied here since the source had always been the
 * low-latency side in every earlier test).
 *
 * <p>Stream keys are still fetched one at a time: XRANGE pagination depends on the previous
 * page's last id, so a stream's fetch is inherently sequential per key and doesn't pipeline
 * across keys the way a single-command-per-key fetch does.
 */
@Tags({"redis", "migration", "deserialize", "source"})
@CapabilityDescription("Reads the full value of a key emitted by RedisScanReader and writes it as a "
        + "structured JSON payload into the FlowFile content, per spec section 4.2. Pipelines "
        + "string/hash/list/set/zset reads across a batch of keys per trigger instead of one "
        + "blocking round trip per key. Stream keys are still fetched one at a time.")
public class RedisTypeDeserializer extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .required(true)
            .identifiesControllerService(RedisConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor HASH_FIELD_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("hash-field-batch-size")
            .displayName("Hash Field Batch Size")
            .description("Max fields per emitted FlowFile for a hash; larger hashes are split across "
                    + "multiple chunked FlowFiles that RedisBatchWriter re-assembles before writing.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();

    public static final PropertyDescriptor LIST_CHUNK_SIZE = new PropertyDescriptor.Builder()
            .name("list-chunk-size")
            .displayName("List Chunk Size")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5000")
            .build();

    public static final PropertyDescriptor STREAM_READ_COUNT = new PropertyDescriptor.Builder()
            .name("stream-read-count")
            .displayName("Stream Read Count")
            .description("Max entries fetched per XRANGE call while paginating a stream's full history.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor INCLUDE_CONSUMER_GROUPS = new PropertyDescriptor.Builder()
            .name("include-consumer-groups")
            .displayName("Include Consumer Groups")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("Batch Size")
            .description("Number of FlowFiles to pipeline reads for per trigger, instead of one "
                    + "blocking round trip per key. Applies to string/hash/list/set/zset keys - "
                    + "stream keys are always fetched one at a time (see the processor description).")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("500")
            .build();

    public static final PropertyDescriptor BATCH_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("batch-timeout-ms")
            .displayName("Batch Timeout (ms)")
            .description("Max time to await one batch's pipelined reads.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();

    public static final PropertyDescriptor DFLY_TO_DFLY = new PropertyDescriptor.Builder()
            .name("dfly-to-dfly")
            .displayName("Dragonfly-to-Dragonfly Optimizations")
            .description("Enable when both source and target are Dragonfly. For string/hash/list/"
                    + "set/zset keys, reads via DUMP - a byte-for-byte copy of the key's internal "
                    + "serialization - instead of the type-specific GET/HGETALL/LRANGE/SMEMBERS/"
                    + "ZRANGE read, paired with RedisBatchWriter issuing a single RESTORE instead "
                    + "of a write command whose argument count scales with the key's own size "
                    + "(e.g. a multi-million-member zset becoming one 10,000,002-argument ZADD - "
                    + "RESTORE's argument count is fixed regardless of element count). Falls back "
                    + "to the normal per-type read for any key whose DUMP payload exceeds Max Dump "
                    + "Payload Bytes. Stream keys are unaffected - already written entry-by-entry, "
                    + "never had this problem. Only safe between Dragonfly instances on compatible "
                    + "versions - DUMP payloads aren't a portable format. Mirrors ModuleTypeHandler's "
                    + "own --dfly-to-dfly, which covers ReJSON-RL/TopK/Bloom/CMS keys separately.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MAX_DUMP_PAYLOAD_BYTES = new PropertyDescriptor.Builder()
            .name("max-dump-payload-bytes")
            .displayName("Max Dump Payload Bytes")
            .description("Under Dragonfly-to-Dragonfly Optimizations, a key whose DUMP payload "
                    + "exceeds this many bytes falls back to the normal type-specific read instead "
                    + "- a defensive cap against the target's max-bulk-string-length limit (a "
                    + "different, much larger limit than the multibulk-element-count one DUMP/"
                    + "RESTORE itself avoids), independent of how many elements the key contains.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("67108864")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Value read and serialised successfully").build();
    public static final Relationship REL_KEY_MISSING = new Relationship.Builder().name("key_missing").description("Key expired between SCAN and fetch").build();
    public static final Relationship REL_MODULE_TYPE = new Relationship.Builder().name("module_type").description("Key is a module type (ReJSON-RL, TopK, etc.)").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Read failed").build();

    private static final Set<String> CORE_TYPES = Set.of("string", "hash", "list", "set", "zset", "stream");

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REDIS_CONNECTION_POOL, HASH_FIELD_BATCH_SIZE, LIST_CHUNK_SIZE, STREAM_READ_COUNT,
            INCLUDE_CONSUMER_GROUPS, BATCH_SIZE, BATCH_TIMEOUT_MS, DFLY_TO_DFLY, MAX_DUMP_PAYLOAD_BYTES);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_KEY_MISSING, REL_MODULE_TYPE, REL_FAILURE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /** One FlowFile's worth of context, carried through the batched read phase. */
    private record Item(FlowFile flowFile, String key, String type, long ttlMs, String encoding) {
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        long batchTimeoutMs = context.getProperty(BATCH_TIMEOUT_MS).asLong();
        List<FlowFile> batch = session.get(batchSize);
        if (batch.isEmpty()) {
            context.yield();
            return;
        }

        RedisConnectionPoolService redisPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        int hashFieldBatchSize = context.getProperty(HASH_FIELD_BATCH_SIZE).asInteger();
        int listChunkSize = context.getProperty(LIST_CHUNK_SIZE).asInteger();
        int streamReadCount = context.getProperty(STREAM_READ_COUNT).asInteger();
        boolean includeConsumerGroups = context.getProperty(INCLUDE_CONSUMER_GROUPS).asBoolean();
        boolean dflyToDfly = context.getProperty(DFLY_TO_DFLY).asBoolean();
        long maxDumpPayloadBytes = context.getProperty(MAX_DUMP_PAYLOAD_BYTES).asLong();
        ComponentLog logger = getLogger();

        List<Item> pipelineItems = new ArrayList<>();
        List<Item> streamItems = new ArrayList<>();

        for (FlowFile flowFile : batch) {
            String key = flowFile.getAttribute("redis.key");
            String type = flowFile.getAttribute("redis.type");
            String encoding = flowFile.getAttribute("redis.encoding");
            long ttlMs = parseLong(flowFile.getAttribute("redis.ttl.ms"), -1);

            if (!CORE_TYPES.contains(type)) {
                session.transfer(flowFile, REL_MODULE_TYPE);
                continue;
            }
            Item item = new Item(flowFile, key, type, ttlMs, encoding);
            if ("stream".equals(type)) {
                streamItems.add(item);
            } else {
                pipelineItems.add(item);
            }
        }

        if (!pipelineItems.isEmpty()) {
            processPipelinedBatch(redisPool, pipelineItems, hashFieldBatchSize, listChunkSize, batchTimeoutMs, session, logger,
                    dflyToDfly, maxDumpPayloadBytes);
        }
        for (Item item : streamItems) {
            processStream(redisPool, item, streamReadCount, includeConsumerGroups, session, logger);
        }
        session.commit();
    }

    /** Entry point for the single-command-per-key types (string/hash/list/set/zset). Under
     * dfly-to-dfly, every item DUMPs first (see class javadoc on Dragonfly-to-Dragonfly
     * Optimizations); any key whose payload exceeds Max Dump Payload Bytes falls back to
     * {@link #processNormalItems} - a real fallback (a second read), not just a failure route,
     * so an oversized DUMP payload never leaves a key unmigrated when the normal chunked-write
     * path would have handled it fine. Off, every item goes straight to processNormalItems. */
    private void processPipelinedBatch(RedisConnectionPoolService redisPool, List<Item> items, int hashFieldBatchSize,
                                        int listChunkSize, long batchTimeoutMs, ProcessSession session, ComponentLog logger,
                                        boolean dflyToDfly, long maxDumpPayloadBytes) {
        if (!dflyToDfly) {
            processNormalItems(redisPool, items, hashFieldBatchSize, listChunkSize, batchTimeoutMs, session, logger);
            return;
        }

        Map<Item, RedisFuture<byte[]>> futures = redisPool.withConnection(cmds -> {
            Map<Item, RedisFuture<byte[]>> f = new LinkedHashMap<>();
            for (Item item : items) {
                f.put(item, cmds.dump(item.key().getBytes(StandardCharsets.UTF_8)));
            }
            awaitAllTolerantly(f.values().toArray(new RedisFuture[0]), batchTimeoutMs);
            return f;
        });

        List<Item> fallbackItems = new ArrayList<>();
        for (Item item : items) {
            RedisFuture<byte[]> future = futures.get(item);
            byte[] dumpPayload;
            try {
                if (future == null || !future.isDone() || future.getError() != null) {
                    throw new ProcessException(future == null ? "no future" : future.getError());
                }
                dumpPayload = future.get();
            } catch (Exception e) {
                logger.error("Failed to DUMP key {}", item.key(), e);
                session.transfer(session.penalize(item.flowFile()), REL_FAILURE);
                continue;
            }

            if (dumpPayload == null) {
                session.transfer(item.flowFile(), REL_KEY_MISSING);
                continue;
            }
            if (dumpPayload.length > maxDumpPayloadBytes) {
                logger.warn("DUMP payload for key {} ({} bytes) exceeds Max Dump Payload Bytes ({}) - "
                                + "falling back to the normal type-specific read for this key",
                        item.key(), dumpPayload.length, maxDumpPayloadBytes);
                fallbackItems.add(item);
                continue;
            }
            emitDump(session, item.flowFile(), dumpPayload);
        }

        if (!fallbackItems.isEmpty()) {
            processNormalItems(redisPool, fallbackItems, hashFieldBatchSize, listChunkSize, batchTimeoutMs, session, logger);
        }
    }

    /** Dispatches the type-specific single-command-per-key read (GET/HGETALL/LRANGE/SMEMBERS/
     * ZRANGE) for every item concurrently on one borrowed connection, awaits them all together,
     * then resolves and serializes each individually. Used both as the only path when
     * dfly-to-dfly is off, and as the fallback for any key whose DUMP payload was too large. */
    private void processNormalItems(RedisConnectionPoolService redisPool, List<Item> items, int hashFieldBatchSize,
                                     int listChunkSize, long batchTimeoutMs, ProcessSession session, ComponentLog logger) {
        Map<Item, RedisFuture<?>> futures = redisPool.withConnection(cmds -> {
            Map<Item, RedisFuture<?>> f = new LinkedHashMap<>();
            for (Item item : items) {
                f.put(item, dispatch(cmds, item));
            }
            awaitAllTolerantly(f.values().toArray(new RedisFuture[0]), batchTimeoutMs);
            return f;
        });

        for (Item item : items) {
            RedisFuture<?> future = futures.get(item);
            Object raw;
            try {
                if (future == null || !future.isDone() || future.getError() != null) {
                    throw new ProcessException(future == null ? "no future" : future.getError());
                }
                raw = future.get();
            } catch (Exception e) {
                logger.error("Failed to read value for key {}", item.key(), e);
                session.transfer(session.penalize(item.flowFile()), REL_FAILURE);
                continue;
            }

            Optional<List<KeyRecord>> records;
            try {
                records = buildRecords(item, raw, hashFieldBatchSize, listChunkSize);
            } catch (Exception e) {
                logger.error("Failed to serialize value for key {}", item.key(), e);
                session.transfer(session.penalize(item.flowFile()), REL_FAILURE);
                continue;
            }

            if (records.isEmpty()) {
                session.transfer(item.flowFile(), REL_KEY_MISSING);
                continue;
            }
            emitChunks(session, item.flowFile(), item.key(), records.get());
        }
    }

    /** Writes a DUMP payload straight to FlowFile content (no JSON envelope - key/type/ttl/
     * encoding are already FlowFile attributes, inherited from the original via session.create)
     * tagged with redis.dfly-dump so RedisBatchWriter issues a RESTORE instead of parsing a
     * KeyRecord. */
    private void emitDump(ProcessSession session, FlowFile original, byte[] dumpPayload) {
        FlowFile child = session.create(original);
        child = session.write(child, out -> out.write(dumpPayload));
        child = session.putAttribute(child, "redis.dfly-dump", "true");
        session.transfer(child, REL_SUCCESS);
        session.remove(original);
    }

    private static RedisFuture<?> dispatch(RedisClusterAsyncCommands<byte[], byte[]> cmds, Item item) {
        byte[] keyBytes = item.key().getBytes(StandardCharsets.UTF_8);
        return switch (item.type()) {
            case "string" -> (RedisFuture<?>) cmds.get(keyBytes);
            case "hash" -> (RedisFuture<?>) cmds.hgetall(keyBytes);
            case "list" -> (RedisFuture<?>) cmds.lrange(keyBytes, 0, -1);
            case "set" -> (RedisFuture<?>) cmds.smembers(keyBytes);
            case "zset" -> (RedisFuture<?>) cmds.zrangeWithScores(keyBytes, 0, -1);
            default -> throw new IllegalStateException("unexpected pipelined type: " + item.type());
        };
    }

    @SuppressWarnings("unchecked")
    private static Optional<List<KeyRecord>> buildRecords(Item item, Object raw, int hashFieldBatchSize, int listChunkSize) {
        return switch (item.type()) {
            case "string" -> RedisValueFetcher.buildStringRecord(item.key(), item.ttlMs(), item.encoding(), (byte[]) raw);
            case "hash" -> RedisValueFetcher.buildHashRecord(item.key(), item.ttlMs(), item.encoding(), (Map<byte[], byte[]>) raw, hashFieldBatchSize);
            case "list" -> RedisValueFetcher.buildListRecord(item.key(), item.ttlMs(), item.encoding(), (List<byte[]>) raw, listChunkSize);
            case "set" -> RedisValueFetcher.buildSetRecord(item.key(), item.ttlMs(), item.encoding(), (Set<byte[]>) raw);
            case "zset" -> RedisValueFetcher.buildZsetRecord(item.key(), item.ttlMs(), item.encoding(), (List<ScoredValue<byte[]>>) raw);
            default -> throw new IllegalStateException("unexpected pipelined type: " + item.type());
        };
    }

    /** Streams paginate via XRANGE, where each page's range depends on the previous page's last
     * id, so they can't pipeline across keys the way the single-command types above do - fetched
     * one at a time, same as before this batching change. */
    private void processStream(RedisConnectionPoolService redisPool, Item item, int streamReadCount,
                                boolean includeConsumerGroups, ProcessSession session, ComponentLog logger) {
        Optional<List<KeyRecord>> records;
        try {
            records = redisPool.withConnection(cmds -> {
                try {
                    return RedisValueFetcher.fetch(cmds, item.key(), item.type(), item.ttlMs(), item.encoding(),
                            0, 0, streamReadCount, includeConsumerGroups);
                } catch (Exception e) {
                    throw new ProcessException(e);
                }
            });
        } catch (Exception e) {
            logger.error("Failed to read value for key {}", item.key(), e);
            session.transfer(session.penalize(item.flowFile()), REL_FAILURE);
            return;
        }

        if (records.isEmpty()) {
            session.transfer(item.flowFile(), REL_KEY_MISSING);
            return;
        }
        emitChunks(session, item.flowFile(), item.key(), records.get());
    }

    private void emitChunks(ProcessSession session, FlowFile original, String key, List<KeyRecord> chunks) {
        for (int i = 0; i < chunks.size(); i++) {
            FlowFile child = session.create(original);
            KeyRecord record = chunks.get(i);
            try {
                child = session.write(child, out -> RedisTypeSerializer.writeJson(record, out));
            } catch (Exception e) {
                getLogger().error("Failed to serialize value for key {}", key, e);
                session.transfer(session.penalize(child), REL_FAILURE);
                continue;
            }
            if (chunks.size() > 1) {
                Map<String, String> chunkAttrs = new HashMap<>();
                chunkAttrs.put("redis.chunk.index", String.valueOf(i));
                chunkAttrs.put("redis.chunk.total", String.valueOf(chunks.size()));
                child = session.putAllAttributes(child, chunkAttrs);
            }
            session.transfer(child, REL_SUCCESS);
        }
        session.remove(original);
    }

    // LettuceFutures.awaitAll throws if any future completed exceptionally, which would
    // otherwise abort this whole batch instead of letting each item's failure be routed
    // individually below (same fix as RedisScanReader.awaitAllTolerantly).
    private static void awaitAllTolerantly(RedisFuture<?>[] futures, long timeoutMs) {
        try {
            LettuceFutures.awaitAll(Duration.ofMillis(timeoutMs), futures);
        } catch (Exception ignored) {
            // resolved (and any failure handled) per-future below
        }
    }

    private static long parseLong(String value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
