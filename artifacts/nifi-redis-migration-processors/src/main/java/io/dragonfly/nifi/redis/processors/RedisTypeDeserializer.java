package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.DragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.CommandBuilder;
import io.dragonfly.nifi.redis.util.FailureReason;
import io.dragonfly.nifi.redis.util.Futures;
import io.dragonfly.nifi.redis.util.KeyFilter;
import io.dragonfly.nifi.redis.util.KeyRecord;
import io.dragonfly.nifi.redis.util.RedisBytes;
import io.dragonfly.nifi.redis.util.RedisTypeSerializer;
import io.dragonfly.nifi.redis.util.RedisValueFetcher;
import io.dragonfly.nifi.redis.util.RestoreProbe;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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

    public static final PropertyDescriptor RESTORE_PROBE_TARGET = new PropertyDescriptor.Builder()
            .name("restore-probe-target")
            .displayName("RESTORE Probe Target")
            .description("The migration target. When set, the first key of each type and encoding is "
                    + "DUMPed, RESTOREd to a scratch key on this target and compared with a typed read of "
                    + "both sides. Keys of a type and encoding that pass are copied with DUMP/RESTORE "
                    + "(streams keep their consumers and pending entries); the rest use the typed read. "
                    + "The verdicts are logged. Leave unset to use only the typed read, unless "
                    + "Dragonfly-to-Dragonfly Optimizations forces DUMP without a probe.")
            .required(false)
            .identifiesControllerService(DragonflyConnectionPoolService.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Value read and serialised successfully").build();
    public static final Relationship REL_KEY_MISSING = new Relationship.Builder().name("key_missing").description("Key expired between SCAN and fetch").build();
    public static final Relationship REL_MODULE_TYPE = new Relationship.Builder().name("module_type").description("Key is a module type (ReJSON-RL, TopK, etc.)").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Read failed").build();

    private static final Set<String> CORE_TYPES = Set.of("string", "hash", "list", "set", "zset", "stream");

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REDIS_CONNECTION_POOL, HASH_FIELD_BATCH_SIZE, LIST_CHUNK_SIZE, STREAM_READ_COUNT,
            INCLUDE_CONSUMER_GROUPS, BATCH_SIZE, BATCH_TIMEOUT_MS, DFLY_TO_DFLY, MAX_DUMP_PAYLOAD_BYTES,
            RESTORE_PROBE_TARGET, RedisSingleKeyFetch.KEY_TYPE_FILTER);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_KEY_MISSING, REL_MODULE_TYPE, REL_FAILURE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /** Null when no probe target is configured. Rebuilt on every start so a changed target is re-probed. */
    private volatile RestoreProbe restoreProbe;

    private volatile RedisValueFetcher.FieldTtlSupport fieldTtls = new RedisValueFetcher.FieldTtlSupport();

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        fieldTtls = new RedisValueFetcher.FieldTtlSupport();
        restoreProbe = context.getProperty(RESTORE_PROBE_TARGET).isSet()
                ? new RestoreProbe(context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class),
                        context.getProperty(RESTORE_PROBE_TARGET).asControllerService(DragonflyConnectionPoolService.class))
                : null;
    }

    /** One FlowFile's worth of context, carried through the batched read phase. */
    private record Item(FlowFile flowFile, byte[] key, String type, long ttlMs, String encoding) {
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
        batch = resolveMissingTypes(redisPool, batch, batchTimeoutMs, session,
                KeyFilter.parse(context.getProperty(RedisSingleKeyFetch.KEY_TYPE_FILTER).getValue(), null, null));

        List<Item> dumpItems = new ArrayList<>();
        List<Item> pipelineItems = new ArrayList<>();
        List<Item> streamItems = new ArrayList<>();
        RestoreProbe probe = restoreProbe;

        for (FlowFile flowFile : batch) {
            byte[] key = RedisBytes.key(flowFile);
            String type = flowFile.getAttribute("redis.type");
            String encoding = flowFile.getAttribute("redis.encoding");
            long ttlMs = parseLong(flowFile.getAttribute("redis.ttl.ms"), -1);

            if (!CORE_TYPES.contains(type)) {
                session.transfer(flowFile, REL_MODULE_TYPE);
                continue;
            }
            // Returned by RedisBatchWriter's restore_rejected: still carries its DUMP content and
            // marker, which the typed children created from it must not inherit.
            boolean rejected = "true".equals(flowFile.getAttribute("redis.restore.rejected"));
            if (rejected) {
                flowFile = session.removeAttribute(flowFile, "redis.dfly-dump");
            }
            Item item = new Item(flowFile, key, type, ttlMs, encoding);
            boolean viaDump = !rejected && ((dflyToDfly && !"stream".equals(type))
                    || (probe != null && probe.verdictFor(key, type, encoding, this::reportVerdict).accepted()));
            if (viaDump) {
                dumpItems.add(item);
            } else if ("stream".equals(type)) {
                streamItems.add(item);
            } else {
                pipelineItems.add(item);
            }
        }
        session.adjustCounter("Keys Read for RESTORE", dumpItems.size(), false);
        session.adjustCounter("Keys Read for Typed Write", pipelineItems.size() + streamItems.size(), false);

        if (!dumpItems.isEmpty()) {
            List<Item> fallback = processDumpItems(redisPool, dumpItems, batchTimeoutMs, session, logger, maxDumpPayloadBytes);
            for (Item item : fallback) {
                ("stream".equals(item.type()) ? streamItems : pipelineItems).add(item);
            }
        }
        if (!pipelineItems.isEmpty()) {
            processNormalItems(redisPool, pipelineItems, hashFieldBatchSize, listChunkSize, batchTimeoutMs, session, logger);
        }
        for (Item item : streamItems) {
            processStream(redisPool, item, streamReadCount, includeConsumerGroups, session, logger);
        }
        session.commit();
    }

    /**
     * FlowFiles from RedisReplicationReader carry only redis.key: their type is whatever the key is
     * now, read here with PTTL in one pipelined round. A key that is gone becomes a tombstone on
     * success rather than going to key_missing: on its own relationship a delete could overtake, or
     * be overtaken by, a later write of the same key, while on success both travel through the
     * writer's one queue in order. Key Type Filter applies to these only (scan FlowFiles were
     * filtered by RedisScanReader already), and a key whose type is now outside it is deleted the
     * same way: it may be an earlier copy, made while the key still had an allowed type.
     */
    private List<FlowFile> resolveMissingTypes(RedisConnectionPoolService redisPool, List<FlowFile> batch, long batchTimeoutMs,
                                               ProcessSession session, KeyFilter typeFilter) {
        List<FlowFile> untyped = batch.stream().filter(f -> f.getAttribute("redis.type") == null).toList();
        if (untyped.isEmpty()) {
            return batch;
        }
        Map<FlowFile, RedisFuture<String>> types = new LinkedHashMap<>();
        Map<FlowFile, RedisFuture<Long>> ttls = new HashMap<>();
        Map<FlowFile, RedisFuture<String>> encodings = new HashMap<>();
        redisPool.withConnection(cmds -> {
            for (FlowFile flowFile : untyped) {
                byte[] key = RedisBytes.key(flowFile);
                types.put(flowFile, cmds.type(key));
                ttls.put(flowFile, cmds.pttl(key));
                encodings.put(flowFile, cmds.objectEncoding(key));
            }
            List<RedisFuture<?>> all = new ArrayList<>(types.values());
            all.addAll(ttls.values());
            all.addAll(encodings.values());
            Futures.awaitAll(all.toArray(new RedisFuture[0]), batchTimeoutMs);
            return null;
        });
        List<FlowFile> resolved = new ArrayList<>(batch.size());
        for (FlowFile flowFile : batch) {
            if (!types.containsKey(flowFile)) {
                resolved.add(flowFile);
                continue;
            }
            String type;
            long ttlMs;
            try {
                type = types.get(flowFile).get();
                ttlMs = ttls.get(flowFile).get();
            } catch (Exception e) {
                getLogger().error("Failed to read TYPE/PTTL for key {}", RedisBytes.display(RedisBytes.key(flowFile)), e);
                session.transfer(session.penalize(FailureReason.tag(session, flowFile, "TYPE or PTTL on the source failed", e)), REL_FAILURE);
                continue;
            }
            if ("none".equals(type) || !typeFilter.allowsType(type)) {
                if (!"none".equals(type)) {
                    session.adjustCounter("Keys Skipped by Type Filter (" + type + ")", 1, false);
                }
                byte[] key = RedisBytes.key(flowFile);
                FlowFile tombstone = session.write(session.putAttribute(flowFile, "redis.type", KeyRecord.DELETED),
                        out -> RedisTypeSerializer.writeJson(KeyRecord.of(key, false, KeyRecord.DELETED, -1, null, null), out));
                session.transfer(tombstone, REL_SUCCESS);
            } else {
                Map<String, String> attrs = new HashMap<>(Map.of("redis.type", type, "redis.ttl.ms", String.valueOf(ttlMs),
                        CommandBuilder.TTL_READ_AT, String.valueOf(System.currentTimeMillis())));
                // The RESTORE probe keys its verdicts by encoding (a Redis 7.4 hash with field TTLs is
                // listpackex and restores nowhere); a source without OBJECT ENCODING only loses that detail.
                try {
                    String encoding = encodings.get(flowFile).get();
                    if (encoding != null) {
                        attrs.put("redis.encoding", encoding);
                    }
                } catch (Exception ignored) {
                    // the verdict falls back to type/unknown, and the restore_rejected loop covers a wrong guess
                }
                resolved.add(session.putAllAttributes(flowFile, attrs));
            }
        }
        return resolved;
    }

    private void reportVerdict(String typeAndEncoding, String sampleKey, RestoreProbe.Verdict verdict) {
        if (verdict.accepted()) {
            getLogger().info("RESTORE probe for {} (sample key {}): {}; these keys use DUMP/RESTORE", typeAndEncoding, sampleKey, verdict.detail());
        } else {
            getLogger().warn("RESTORE probe for {} (sample key {}): {}; these keys use the typed read", typeAndEncoding, sampleKey, verdict.detail());
        }
    }

    /** DUMPs every item in one pipeline and emits the payloads. Returns the items whose payload
     * exceeds Max Dump Payload Bytes, for the caller to copy with the typed read instead - a real
     * fallback, so an oversized payload never leaves a key unmigrated. */
    private List<Item> processDumpItems(RedisConnectionPoolService redisPool, List<Item> items, long batchTimeoutMs,
                                        ProcessSession session, ComponentLog logger, long maxDumpPayloadBytes) {
        Map<Item, RedisFuture<byte[]>> futures = redisPool.withConnection(cmds -> {
            Map<Item, RedisFuture<byte[]>> f = new LinkedHashMap<>();
            for (Item item : items) {
                f.put(item, cmds.dump(item.key()));
            }
            Futures.awaitAll(f.values().toArray(new RedisFuture[0]), batchTimeoutMs);
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
                logger.error("Failed to DUMP key {}", RedisBytes.display(item.key()), e);
                session.transfer(session.penalize(FailureReason.tag(session, item.flowFile(), "DUMP on the source failed", e)), REL_FAILURE);
                continue;
            }

            if (dumpPayload == null) {
                session.transfer(item.flowFile(), REL_KEY_MISSING);
                continue;
            }
            if (dumpPayload.length > maxDumpPayloadBytes) {
                logger.warn("DUMP payload for key {} ({} bytes) exceeds Max Dump Payload Bytes ({}) - "
                                + "falling back to the normal type-specific read for this key",
                        RedisBytes.display(item.key()), dumpPayload.length, maxDumpPayloadBytes);
                fallbackItems.add(item);
                continue;
            }
            emitDump(session, item.flowFile(), dumpPayload);
        }
        return fallbackItems;
    }

    /** Dispatches the type-specific single-command-per-key read (GET/HGETALL/LRANGE/SMEMBERS/
     * ZRANGE) for every item concurrently on one borrowed connection, awaits them all together,
     * then resolves and serializes each individually. */
    private void processNormalItems(RedisConnectionPoolService redisPool, List<Item> items, int hashFieldBatchSize,
                                     int listChunkSize, long batchTimeoutMs, ProcessSession session, ComponentLog logger) {
        Map<Item, RedisFuture<?>> futures = redisPool.withConnection(cmds -> {
            Map<Item, RedisFuture<?>> f = new LinkedHashMap<>();
            for (Item item : items) {
                f.put(item, dispatch(cmds, item));
            }
            Futures.awaitAll(f.values().toArray(new RedisFuture[0]), batchTimeoutMs);
            return f;
        });
        Map<Item, Map<String, Long>> fieldExpiries = readFieldExpiries(redisPool, items, futures, batchTimeoutMs, logger);

        for (Item item : items) {
            RedisFuture<?> future = futures.get(item);
            Object raw;
            try {
                if (future == null || !future.isDone() || future.getError() != null) {
                    throw new ProcessException(future == null ? "no future" : future.getError());
                }
                raw = future.get();
            } catch (Exception e) {
                logger.error("Failed to read value for key {}", RedisBytes.display(item.key()), e);
                session.transfer(session.penalize(FailureReason.tag(session, item.flowFile(), "reading the value from the source failed", e)), REL_FAILURE);
                continue;
            }

            Optional<List<KeyRecord>> records;
            try {
                records = buildRecords(item, raw, hashFieldBatchSize, listChunkSize, fieldExpiries.getOrDefault(item, Map.of()));
            } catch (Exception e) {
                logger.error("Failed to serialize value for key {}", RedisBytes.display(item.key()), e);
                session.transfer(session.penalize(FailureReason.tag(session, item.flowFile(), "serializing the value failed", e)), REL_FAILURE);
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
        byte[] keyBytes = item.key();
        return switch (item.type()) {
            case "string" -> (RedisFuture<?>) cmds.get(keyBytes);
            case "hash" -> (RedisFuture<?>) cmds.hgetall(keyBytes);
            case "list" -> (RedisFuture<?>) cmds.lrange(keyBytes, 0, -1);
            case "set" -> (RedisFuture<?>) cmds.smembers(keyBytes);
            case "zset" -> (RedisFuture<?>) cmds.zrangeWithScores(keyBytes, 0, -1);
            default -> throw new IllegalStateException("unexpected pipelined type: " + item.type());
        };
    }

    /** A second pipelined round, for the hashes in this batch only: HPTTL needs the field
     * names the first round returned. A failure here loses no data, so it only drops the TTLs of
     * that hash and is logged. */
    @SuppressWarnings("unchecked")
    private Map<Item, Map<String, Long>> readFieldExpiries(RedisConnectionPoolService redisPool, List<Item> items,
                                                           Map<Item, RedisFuture<?>> reads, long batchTimeoutMs, ComponentLog logger) {
        RedisValueFetcher.FieldTtlSupport support = fieldTtls;
        if (!support.supported()) {
            return Map.of();
        }
        Map<Item, List<Map.Entry<List<String>, RedisFuture<List<Long>>>>> requests = redisPool.withConnection(cmds -> {
            Map<Item, List<Map.Entry<List<String>, RedisFuture<List<Long>>>>> r = new LinkedHashMap<>();
            for (Item item : items) {
                RedisFuture<?> read = reads.get(item);
                if ("hash".equals(item.type()) && read != null && read.isDone() && read.getError() == null) {
                    try {
                        r.put(item, RedisValueFetcher.requestFieldExpiries(cmds, item.key(), ((Map<byte[], byte[]>) read.get()).keySet(), support));
                    } catch (Exception e) {
                        logger.warn("Could not request field TTLs for hash {}", RedisBytes.display(item.key()), e);
                    }
                }
            }
            Futures.awaitAll(r.values().stream().flatMap(List::stream).map(Map.Entry::getValue).toArray(RedisFuture[]::new), batchTimeoutMs);
            return r;
        });
        Map<Item, Map<String, Long>> expiries = new HashMap<>();
        requests.forEach((item, request) -> {
            try {
                expiries.put(item, RedisValueFetcher.awaitFieldExpiries(request, support));
            } catch (Exception e) {
                logger.warn("Could not read field TTLs for hash {}; copying it without them", RedisBytes.display(item.key()), e);
            }
        });
        return expiries;
    }

    @SuppressWarnings("unchecked")
    private static Optional<List<KeyRecord>> buildRecords(Item item, Object raw, int hashFieldBatchSize, int listChunkSize,
                                                          Map<String, Long> fieldExpireAtMs) {
        return switch (item.type()) {
            case "string" -> RedisValueFetcher.buildStringRecord(item.key(), item.ttlMs(), item.encoding(), (byte[]) raw);
            case "hash" -> RedisValueFetcher.buildHashRecord(item.key(), item.ttlMs(), item.encoding(), (Map<byte[], byte[]>) raw,
                    hashFieldBatchSize, fieldExpireAtMs);
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
            logger.error("Failed to read value for key {}", RedisBytes.display(item.key()), e);
            session.transfer(session.penalize(FailureReason.tag(session, item.flowFile(), "reading the value from the source failed", e)), REL_FAILURE);
            return;
        }

        if (records.isEmpty()) {
            session.transfer(item.flowFile(), REL_KEY_MISSING);
            return;
        }
        emitChunks(session, item.flowFile(), item.key(), records.get());
    }

    private void emitChunks(ProcessSession session, FlowFile original, byte[] key, List<KeyRecord> chunks) {
        for (int i = 0; i < chunks.size(); i++) {
            FlowFile child = session.create(original);
            KeyRecord record = chunks.get(i);
            try {
                child = session.write(child, out -> RedisTypeSerializer.writeJson(record, out));
            } catch (Exception e) {
                getLogger().error("Failed to serialize value for key {}", RedisBytes.display(key), e);
                session.transfer(session.penalize(FailureReason.tag(session, child, "serializing the value failed", e)), REL_FAILURE);
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
