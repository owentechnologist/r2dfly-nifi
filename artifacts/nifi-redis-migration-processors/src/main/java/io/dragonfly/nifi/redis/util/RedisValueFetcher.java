package io.dragonfly.nifi.redis.util;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    private RedisValueFetcher() {
    }

    public static Optional<List<KeyRecord>> fetch(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, String type, long ttlMs, String encoding,
            int hashFieldBatchSize, int listChunkSize, int streamReadCount, boolean includeConsumerGroups) {
        try {
            return switch (type) {
                case "string" -> fetchString(cmds, key, ttlMs, encoding);
                case "hash" -> fetchHash(cmds, key, ttlMs, encoding, hashFieldBatchSize);
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
            RedisClusterAsyncCommands<byte[], byte[]> cmds, String key, long ttlMs, String encoding, int chunkSize) throws Exception {
        Map<byte[], byte[]> raw = cmds.hgetall(key.getBytes(UTF_8)).get();
        return buildHashRecord(key, ttlMs, encoding, raw, chunkSize);
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
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        Map<String, Object> fields = new LinkedHashMap<>();
        raw.forEach((k, v) -> fields.put(new String(k, UTF_8), new String(v, UTF_8)));
        return Optional.of(chunkMap(key, ttlMs, encoding, fields, chunkSize));
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
        if (entries.isEmpty()) {
            return Optional.empty();
        }
        KeyRecord record = new KeyRecord(key, "stream", ttlMs, encoding, entries);
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
            List<io.lettuce.core.models.stream.PendingMessage> pending =
                    cmds.xpending(key, groupName, Range.unbounded(), Limit.from(10_000)).get();
            List<Object> pendingList = new ArrayList<>();
            for (io.lettuce.core.models.stream.PendingMessage pm : pending) {
                Map<String, Object> pendingEntry = new LinkedHashMap<>();
                pendingEntry.put("id", pm.getId());
                pendingEntry.put("consumer", pm.getConsumer());
                pendingEntry.put("ms_since_last_delivery", pm.getMsSinceLastDelivery());
                pendingEntry.put("redelivery_count", pm.getRedeliveryCount());
                pendingList.add(pendingEntry);
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
