package io.dragonfly.nifi.redis.util;

import io.lettuce.core.Limit;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.IntegerListOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fetches the full current value of a key by type and builds the {@link KeyRecord}(s) for it,
 * factored out of {@code RedisTypeDeserializer} so {@code RedisSingleKeyFetch} can reuse the
 * exact same type-dispatch logic per spec section 4.5, rather than duplicating it.
 *
 * <p>Oversized hashes/lists are split into multiple {@link KeyRecord}s (spec section 4.2); all
 * other types always yield exactly one. A {@code null} return (single-element list containing
 * {@code null}) signals the key vanished between SCAN and fetch ({@code key_missing}).
 */
public final class RedisValueFetcher {

    private static final java.nio.charset.Charset UTF_8 = StandardCharsets.UTF_8;

    /** Fields per HPTTL call, so a huge hash never becomes one enormous command. */
    private static final int FIELD_TTL_BATCH = 1000;

    private static final int PENDING_PAGE = 10_000;

    private enum FieldTtlCommand implements ProtocolKeyword {
        HPTTL;

        @Override
        public byte[] getBytes() {
            return name().getBytes(StandardCharsets.US_ASCII);
        }
    }

    /**
     * Whether the source answers HPTTL (Redis 7.4+, Valkey 9+; not Dragonfly 2.0.2). One instance per
     * processor: the first "unknown command" reply turns it off, so older sources pay one failed
     * call per processor start rather than one per hash.
     */
    public static final class FieldTtlSupport {
        private final AtomicBoolean supported = new AtomicBoolean(true);

        public boolean supported() {
            return supported.get();
        }

        void markUnsupported() {
            supported.set(false);
        }
    }

    private RedisValueFetcher() {
    }

    public static Optional<List<KeyRecord>> fetch(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, String type, long ttlMs, String encoding,
            int hashFieldBatchSize, int listChunkSize, int streamReadCount, boolean includeConsumerGroups) {
        return fetch(cmds, key, type, ttlMs, encoding, hashFieldBatchSize, listChunkSize, streamReadCount, includeConsumerGroups, null);
    }

    /** {@code fieldTtls} null skips reading hash field TTLs. */
    public static Optional<List<KeyRecord>> fetch(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, String type, long ttlMs, String encoding,
            int hashFieldBatchSize, int listChunkSize, int streamReadCount, boolean includeConsumerGroups,
            FieldTtlSupport fieldTtls) {
        try {
            return switch (type) {
                case "string" -> fetchString(cmds, key, ttlMs, encoding);
                case "hash" -> fetchHash(cmds, key, ttlMs, encoding, hashFieldBatchSize, fieldTtls);
                case "list" -> fetchList(cmds, key, ttlMs, encoding, listChunkSize);
                case "set" -> fetchSet(cmds, key, ttlMs, encoding);
                case "zset" -> fetchZset(cmds, key, ttlMs, encoding);
                case "stream" -> fetchStream(cmds, key, ttlMs, encoding, streamReadCount, includeConsumerGroups);
                default -> Optional.empty();
            };
        } catch (Exception e) {
            throw new RedisFetchException("Failed to fetch value for key " + key, e);
        }
    }

    private static Optional<List<KeyRecord>> fetchString(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, long ttlMs, String encoding) throws Exception {
        byte[] value = cmds.get(key.getBytes(UTF_8)).get();
        return buildStringRecord(key, ttlMs, encoding, value);
    }

    private static Optional<List<KeyRecord>> fetchHash(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, long ttlMs, String encoding, int chunkSize,
            FieldTtlSupport fieldTtls) throws Exception {
        Map<byte[], byte[]> raw = cmds.hgetall(key.getBytes(UTF_8)).get();
        Map<String, Long> expiries = fieldTtls == null ? Map.of()
                : awaitFieldExpiries(requestFieldExpiries(cmds, key, raw.keySet(), fieldTtls), fieldTtls);
        return buildHashRecord(key, ttlMs, encoding, raw, chunkSize, expiries);
    }

    /** Dispatches HPTTL for every field without waiting, so a caller can pipeline many hashes. HPTTL
     * rather than HPEXPIRETIME: a relative TTL turns into a deadline on this host's clock, so the
     * source's and target's clocks never have to agree. */
    public static List<Map.Entry<List<String>, RedisFuture<List<Long>>>> requestFieldExpiries(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, Set<byte[]> fields, FieldTtlSupport support) {
        if (!support.supported() || fields.isEmpty()) {
            return List.of();
        }
        List<byte[]> all = new ArrayList<>(fields);
        List<Map.Entry<List<String>, RedisFuture<List<Long>>>> requests = new ArrayList<>();
        for (int start = 0; start < all.size(); start += FIELD_TTL_BATCH) {
            List<byte[]> batch = all.subList(start, Math.min(start + FIELD_TTL_BATCH, all.size()));
            CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE)
                    .addKey(key.getBytes(UTF_8)).add("FIELDS").add(batch.size());
            batch.forEach(args::add);
            requests.add(Map.entry(batch.stream().map(f -> new String(f, UTF_8)).toList(),
                    cmds.dispatch(FieldTtlCommand.HPTTL, new IntegerListOutput<>(ByteArrayCodec.INSTANCE), args)));
        }
        return requests;
    }

    /** Field to its expiry deadline in epoch ms on this host's clock, for fields that have one. */
    public static Map<String, Long> awaitFieldExpiries(
            List<Map.Entry<List<String>, RedisFuture<List<Long>>>> requests, FieldTtlSupport support) throws Exception {
        Map<String, Long> expiries = new LinkedHashMap<>();
        for (Map.Entry<List<String>, RedisFuture<List<Long>>> request : requests) {
            List<Long> reply;
            try {
                reply = request.getValue().get();
            } catch (java.util.concurrent.ExecutionException e) {
                if (e.getCause() instanceof RedisCommandExecutionException rce
                        && String.valueOf(rce.getMessage()).toLowerCase().contains("unknown command")) {
                    support.markUnsupported();
                    return Map.of();
                }
                throw e;
            }
            long readAtMs = System.currentTimeMillis();
            List<String> fields = request.getKey();
            for (int i = 0; i < fields.size() && i < reply.size(); i++) {
                if (reply.get(i) != null && reply.get(i) > 0) {
                    expiries.put(fields.get(i), readAtMs + reply.get(i));
                }
            }
        }
        return expiries;
    }

    private static Optional<List<KeyRecord>> fetchList(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, long ttlMs, String encoding, int chunkSize) throws Exception {
        List<byte[]> raw = cmds.lrange(key.getBytes(UTF_8), 0, -1).get();
        return buildListRecord(key, ttlMs, encoding, raw, chunkSize);
    }

    private static Optional<List<KeyRecord>> fetchSet(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, long ttlMs, String encoding) throws Exception {
        Set<byte[]> raw = cmds.smembers(key.getBytes(UTF_8)).get();
        return buildSetRecord(key, ttlMs, encoding, raw);
    }

    private static Optional<List<KeyRecord>> fetchZset(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, long ttlMs, String encoding) throws Exception {
        List<ScoredValue<byte[]>> raw = cmds.zrangeWithScores(key.getBytes(UTF_8), 0, -1).get();
        return buildZsetRecord(key, ttlMs, encoding, raw);
    }

    /**
     * Builds the {@link KeyRecord}(s) for an already-resolved value, split out from the
     * {@code fetchX} methods above so a caller that pipelines reads for several keys at once
     * (dispatch every key's command concurrently, await them all, then resolve) can reuse the
     * exact same record-building logic without re-fetching one key at a time itself.
     */
    public static Optional<List<KeyRecord>> buildStringRecord(String key, long ttlMs, String encoding, byte[] value) {
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(List.of(new KeyRecord(key, "string", ttlMs, encoding, new String(value, UTF_8))));
    }

    public static Optional<List<KeyRecord>> buildHashRecord(String key, long ttlMs, String encoding, Map<byte[], byte[]> raw, int chunkSize) {
        return buildHashRecord(key, ttlMs, encoding, raw, chunkSize, Map.of());
    }

    public static Optional<List<KeyRecord>> buildHashRecord(String key, long ttlMs, String encoding, Map<byte[], byte[]> raw,
                                                            int chunkSize, Map<String, Long> fieldExpireAtMs) {
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        Map<String, Object> fields = new LinkedHashMap<>();
        raw.forEach((k, v) -> fields.put(new String(k, UTF_8), new String(v, UTF_8)));
        List<KeyRecord> chunks = chunkMap(key, ttlMs, encoding, fields, chunkSize);
        if (!fieldExpireAtMs.isEmpty()) {
            for (KeyRecord chunk : chunks) {
                Map<String, Long> own = new LinkedHashMap<>();
                asFieldMap(chunk.value).keySet().forEach(f -> {
                    Long at = fieldExpireAtMs.get(f);
                    if (at != null) {
                        own.put(f, at);
                    }
                });
                chunk.fieldExpireAtMs = own.isEmpty() ? null : own;
            }
        }
        return Optional.of(chunks);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asFieldMap(Object value) {
        return (Map<String, Object>) value;
    }

    public static Optional<List<KeyRecord>> buildListRecord(String key, long ttlMs, String encoding, List<byte[]> raw, int chunkSize) {
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        List<Object> items = raw.stream().map(b -> (Object) new String(b, UTF_8)).toList();
        return Optional.of(chunkList(key, "list", ttlMs, encoding, items, chunkSize));
    }

    public static Optional<List<KeyRecord>> buildSetRecord(String key, long ttlMs, String encoding, Set<byte[]> raw) {
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        List<Object> items = raw.stream().map(b -> (Object) new String(b, UTF_8)).toList();
        return Optional.of(List.of(new KeyRecord(key, "set", ttlMs, encoding, items)));
    }

    public static Optional<List<KeyRecord>> buildZsetRecord(String key, long ttlMs, String encoding, List<ScoredValue<byte[]>> raw) {
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        List<Object> items = new ArrayList<>();
        for (ScoredValue<byte[]> sv : raw) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("member", new String(sv.getValue(), UTF_8));
            entry.put("score", sv.getScore());
            items.add(entry);
        }
        return Optional.of(List.of(new KeyRecord(key, "zset", ttlMs, encoding, items)));
    }

    private static Optional<List<KeyRecord>> fetchStream(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, long ttlMs, String encoding,
            int readCount, boolean includeConsumerGroups) throws Exception {
        byte[] keyBytes = key.getBytes(UTF_8);
        // Read before the entries: it tells a missing key apart from a stream whose entries were all
        // deleted or trimmed but whose groups and last id still exist and must be copied.
        String lastGeneratedId;
        try {
            Object id = flattenToMap(cmds.xinfoStream(keyBytes).get()).get("last-generated-id");
            lastGeneratedId = id == null ? null : id.toString();
        } catch (java.util.concurrent.ExecutionException e) {
            if (String.valueOf(e.getCause().getMessage()).toLowerCase().contains("no such key")) {
                return Optional.empty();
            }
            throw e;
        }
        List<Object> entries = new ArrayList<>();
        Range<String> range = Range.unbounded();
        String lastId = null;
        while (true) {
            Range<String> pageRange = lastId == null ? range : Range.from(Range.Boundary.excluding(lastId), Range.Boundary.unbounded());
            List<StreamMessage<byte[], byte[]>> page = cmds.xrange(keyBytes, pageRange, Limit.from(readCount)).get();
            if (page.isEmpty()) {
                break;
            }
            for (StreamMessage<byte[], byte[]> message : page) {
                Map<String, Object> fields = new LinkedHashMap<>();
                message.getBody().forEach((k, v) -> fields.put(new String(k, UTF_8), new String(v, UTF_8)));
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("id", message.getId());
                entry.put("fields", fields);
                entries.add(entry);
                lastId = message.getId();
            }
            if (page.size() < readCount) {
                break;
            }
        }
        KeyRecord record = new KeyRecord(key, "stream", ttlMs, encoding, entries);
        record.lastGeneratedId = lastGeneratedId;
        if (includeConsumerGroups) {
            record.consumerGroups = fetchConsumerGroups(cmds, keyBytes);
        }
        return Optional.of(List.of(record));
    }

    private static List<Object> fetchConsumerGroups(RedisClusterAsyncCommands<byte[], byte[]> cmds, byte[] key) throws Exception {
        List<Object> groups = normalizeInfoReply(cmds.xinfoGroups(key).get());
        List<Object> result = new ArrayList<>();
        for (Object g : groups) {
            @SuppressWarnings("unchecked")
            Map<String, Object> group = (Map<String, Object>) g;
            Object nameObj = group.get("name");
            if (nameObj == null) {
                continue;
            }
            byte[] groupName = String.valueOf(nameObj).getBytes(UTF_8);
            group.put("consumers", normalizeInfoReply(cmds.xinfoConsumers(key, groupName).get()));
            List<Object> pendingList = new ArrayList<>();
            String afterId = null;
            while (true) {
                Range<String> pageRange = afterId == null ? Range.unbounded()
                        : Range.from(Range.Boundary.excluding(afterId), Range.Boundary.unbounded());
                List<io.lettuce.core.models.stream.PendingMessage> page =
                        cmds.xpending(key, groupName, pageRange, Limit.from(PENDING_PAGE)).get();
                // The idle time is relative to now; an absolute time stays right however long the
                // record waits before it is written.
                long readAtMs = System.currentTimeMillis();
                for (io.lettuce.core.models.stream.PendingMessage pm : page) {
                    Map<String, Object> pendingEntry = new LinkedHashMap<>();
                    pendingEntry.put("id", pm.getId());
                    pendingEntry.put("consumer", pm.getConsumer());
                    pendingEntry.put("ms_since_last_delivery", pm.getMsSinceLastDelivery());
                    pendingEntry.put("delivered_at_ms", readAtMs - pm.getMsSinceLastDelivery());
                    pendingEntry.put("redelivery_count", pm.getRedeliveryCount());
                    pendingList.add(pendingEntry);
                    afterId = pm.getId();
                }
                if (page.size() < PENDING_PAGE) {
                    break;
                }
            }
            group.put("pending", pendingList);
            result.add(group);
        }
        return result;
    }

    /**
     * Normalizes Lettuce's {@code XINFO GROUPS}/{@code XINFO CONSUMERS} reply into
     * {@code List<Map<String,Object>>}. Lettuce returns these as {@code List<Object>} whose
     * per-entry shape depends on RESP protocol version (RESP3: a {@code Map}; RESP2: a flat
     * list of alternating field name/value) - this normalizer accepts either, since that could
     * not be pinned down to one shape without a live server to verify against.
     */
    @SuppressWarnings("unchecked")
    private static List<Object> normalizeInfoReply(List<Object> reply) {
        List<Object> result = new ArrayList<>();
        for (Object item : reply) {
            if (item instanceof Map<?, ?> map) {
                result.add(normalizeMap((Map<Object, Object>) map));
            } else if (item instanceof List<?> flat) {
                result.add(flattenToMap((List<Object>) flat));
            }
        }
        return result;
    }

    private static Map<String, Object> flattenToMap(List<Object> flat) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < flat.size(); i += 2) {
            map.put(plain(flat.get(i)).toString(), plain(flat.get(i + 1)));
        }
        return map;
    }

    private static Map<String, Object> normalizeMap(Map<Object, Object> raw) {
        Map<String, Object> map = new LinkedHashMap<>();
        raw.forEach((k, v) -> map.put(plain(k).toString(), plain(v)));
        return map;
    }

    private static Object plain(Object o) {
        return (o instanceof byte[] b) ? new String(b, UTF_8) : o;
    }

    private static List<KeyRecord> chunkMap(String key, long ttlMs, String encoding, Map<String, Object> fields, int chunkSize) {
        if (fields.size() <= chunkSize) {
            return List.of(new KeyRecord(key, "hash", ttlMs, encoding, fields));
        }
        List<KeyRecord> chunks = new ArrayList<>();
        Map<String, Object> current = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            current.put(entry.getKey(), entry.getValue());
            if (current.size() == chunkSize) {
                chunks.add(new KeyRecord(key, "hash", ttlMs, encoding, current));
                current = new LinkedHashMap<>();
            }
        }
        if (!current.isEmpty()) {
            chunks.add(new KeyRecord(key, "hash", ttlMs, encoding, current));
        }
        return chunks;
    }

    private static List<KeyRecord> chunkList(String key, String type, long ttlMs, String encoding, List<Object> items, int chunkSize) {
        if (items.size() <= chunkSize) {
            return List.of(new KeyRecord(key, type, ttlMs, encoding, items));
        }
        List<KeyRecord> chunks = new ArrayList<>();
        for (int i = 0; i < items.size(); i += chunkSize) {
            chunks.add(new KeyRecord(key, type, ttlMs, encoding, items.subList(i, Math.min(i + chunkSize, items.size()))));
        }
        return chunks;
    }
}
