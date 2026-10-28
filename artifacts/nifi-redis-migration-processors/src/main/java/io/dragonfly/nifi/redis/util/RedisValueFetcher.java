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
            RedisClusterAsyncCommands<byte[], byte[]> cmds, byte[] key, String type, long ttlMs, String encoding,
            int hashFieldBatchSize, int listChunkSize, int streamReadCount, boolean includeConsumerGroups) {
        return fetch(cmds, key, type, ttlMs, encoding, hashFieldBatchSize, listChunkSize, streamReadCount, includeConsumerGroups, null);
    }

    /** {@code fieldTtls} null skips reading hash field TTLs. */
    public static Optional<List<KeyRecord>> fetch(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, byte[] key, String type, long ttlMs, String encoding,
            int hashFieldBatchSize, int listChunkSize, int streamReadCount, boolean includeConsumerGroups,
            FieldTtlSupport fieldTtls) {
        try {
            return switch (type) {
                case "string" -> buildStringRecord(key, ttlMs, encoding, cmds.get(key).get());
                case "hash" -> fetchHash(cmds, key, ttlMs, encoding, hashFieldBatchSize, fieldTtls);
                case "list" -> buildListRecord(key, ttlMs, encoding, cmds.lrange(key, 0, -1).get(), listChunkSize);
                case "set" -> buildSetRecord(key, ttlMs, encoding, cmds.smembers(key).get());
                case "zset" -> buildZsetRecord(key, ttlMs, encoding, cmds.zrangeWithScores(key, 0, -1).get());
                case "stream" -> fetchStream(cmds, key, ttlMs, encoding, streamReadCount, includeConsumerGroups);
                default -> Optional.empty();
            };
        } catch (Exception e) {
            throw new RedisFetchException("Failed to fetch value for key " + RedisBytes.display(key), e);
        }
    }

    private static Optional<List<KeyRecord>> fetchHash(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, byte[] key, long ttlMs, String encoding, int chunkSize,
            FieldTtlSupport fieldTtls) throws Exception {
        Map<byte[], byte[]> raw = cmds.hgetall(key).get();
        Map<String, Long> expiries = fieldTtls == null ? Map.of()
                : awaitFieldExpiries(requestFieldExpiries(cmds, key, raw.keySet(), fieldTtls), fieldTtls);
        return buildHashRecord(key, ttlMs, encoding, raw, chunkSize, expiries);
    }

    /** Dispatches HPTTL for every field without waiting, so a caller can pipeline many hashes. HPTTL
     * rather than HPEXPIRETIME: a relative TTL turns into a deadline on this host's clock, so the
     * source's and target's clocks never have to agree. Fields are named by {@link RedisBytes#identity}. */
    public static List<Map.Entry<List<String>, RedisFuture<List<Long>>>> requestFieldExpiries(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, byte[] key, Set<byte[]> fields, FieldTtlSupport support) {
        if (!support.supported() || fields.isEmpty()) {
            return List.of();
        }
        List<byte[]> all = new ArrayList<>(fields);
        List<Map.Entry<List<String>, RedisFuture<List<Long>>>> requests = new ArrayList<>();
        for (int start = 0; start < all.size(); start += FIELD_TTL_BATCH) {
            List<byte[]> batch = all.subList(start, Math.min(start + FIELD_TTL_BATCH, all.size()));
            CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE)
                    .addKey(key).add("FIELDS").add(batch.size());
            batch.forEach(args::add);
            requests.add(Map.entry(batch.stream().map(RedisBytes::identity).toList(),
                    cmds.dispatch(FieldTtlCommand.HPTTL, new IntegerListOutput<>(ByteArrayCodec.INSTANCE), args)));
        }
        return requests;
    }

    /** Field (by {@link RedisBytes#identity}) to its expiry deadline in epoch ms on this host's clock, for fields that have one. */
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

    /**
     * Builds the {@link KeyRecord}(s) for an already-resolved value, split out from the fetch
     * methods above so a caller that pipelines reads for several keys at once can reuse the exact
     * same record-building logic. Each decides once, from all the key's bytes, whether the record
     * is base64.
     */
    public static Optional<List<KeyRecord>> buildStringRecord(byte[] key, long ttlMs, String encoding, byte[] value) {
        if (value == null) {
            return Optional.empty();
        }
        boolean base64 = KeyRecord.needsBase64(List.of(value));
        return Optional.of(List.of(KeyRecord.of(key, base64, "string", ttlMs, encoding, KeyRecord.text(value, base64))));
    }

    public static Optional<List<KeyRecord>> buildHashRecord(byte[] key, long ttlMs, String encoding, Map<byte[], byte[]> raw, int chunkSize) {
        return buildHashRecord(key, ttlMs, encoding, raw, chunkSize, Map.of());
    }

    /** {@code fieldExpireAtMs} is keyed by {@link RedisBytes#identity} of each field. */
    public static Optional<List<KeyRecord>> buildHashRecord(byte[] key, long ttlMs, String encoding, Map<byte[], byte[]> raw,
                                                            int chunkSize, Map<String, Long> fieldExpireAtMs) {
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        List<byte[]> all = new ArrayList<>(raw.keySet());
        all.addAll(raw.values());
        boolean base64 = KeyRecord.needsBase64(all);
        Map<String, Object> fields = new LinkedHashMap<>();
        Map<String, Long> expiries = new LinkedHashMap<>();
        raw.forEach((k, v) -> {
            String field = KeyRecord.text(k, base64);
            fields.put(field, KeyRecord.text(v, base64));
            Long at = fieldExpireAtMs.get(RedisBytes.identity(k));
            if (at != null) {
                expiries.put(field, at);
            }
        });
        List<KeyRecord> chunks = chunkMap(key, base64, ttlMs, encoding, fields, chunkSize);
        if (!expiries.isEmpty()) {
            for (KeyRecord chunk : chunks) {
                Map<String, Long> own = new LinkedHashMap<>();
                asFieldMap(chunk.value).keySet().forEach(f -> {
                    Long at = expiries.get(f);
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

    public static Optional<List<KeyRecord>> buildListRecord(byte[] key, long ttlMs, String encoding, List<byte[]> raw, int chunkSize) {
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        boolean base64 = KeyRecord.needsBase64(raw);
        List<Object> items = raw.stream().map(b -> (Object) KeyRecord.text(b, base64)).toList();
        return Optional.of(chunkList(key, base64, "list", ttlMs, encoding, items, chunkSize));
    }

    public static Optional<List<KeyRecord>> buildSetRecord(byte[] key, long ttlMs, String encoding, Set<byte[]> raw) {
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        boolean base64 = KeyRecord.needsBase64(raw);
        List<Object> items = raw.stream().map(b -> (Object) KeyRecord.text(b, base64)).toList();
        return Optional.of(List.of(KeyRecord.of(key, base64, "set", ttlMs, encoding, items)));
    }

    public static Optional<List<KeyRecord>> buildZsetRecord(byte[] key, long ttlMs, String encoding, List<ScoredValue<byte[]>> raw) {
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        boolean base64 = KeyRecord.needsBase64(raw.stream().map(ScoredValue::getValue).toList());
        List<Object> items = new ArrayList<>();
        for (ScoredValue<byte[]> sv : raw) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("member", KeyRecord.text(sv.getValue(), base64));
            entry.put("score", sv.getScore());
            items.add(entry);
        }
        return Optional.of(List.of(KeyRecord.of(key, base64, "zset", ttlMs, encoding, items)));
    }

    private static Optional<List<KeyRecord>> fetchStream(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, byte[] key, long ttlMs, String encoding,
            int readCount, boolean includeConsumerGroups) throws Exception {
        // Read before the entries: it tells a missing key apart from a stream whose entries were all
        // deleted or trimmed but whose groups and last id still exist and must be copied.
        String lastGeneratedId;
        try {
            Object id = flattenToMap(cmds.xinfoStream(key).get()).get("last-generated-id");
            lastGeneratedId = id == null ? null : id.toString();
        } catch (java.util.concurrent.ExecutionException e) {
            if (String.valueOf(e.getCause().getMessage()).toLowerCase().contains("no such key")) {
                return Optional.empty();
            }
            throw e;
        }
        List<StreamMessage<byte[], byte[]>> messages = new ArrayList<>();
        String lastId = null;
        while (true) {
            Range<String> pageRange = lastId == null ? Range.unbounded() : Range.from(Range.Boundary.excluding(lastId), Range.Boundary.unbounded());
            List<StreamMessage<byte[], byte[]>> page = cmds.xrange(key, pageRange, Limit.from(readCount)).get();
            if (page.isEmpty()) {
                break;
            }
            messages.addAll(page);
            lastId = page.get(page.size() - 1).getId();
            if (page.size() < readCount) {
                break;
            }
        }
        List<byte[]> all = new ArrayList<>();
        messages.forEach(m -> m.getBody().forEach((k, v) -> {
            all.add(k);
            all.add(v);
        }));
        boolean base64 = KeyRecord.needsBase64(all);
        List<Object> entries = new ArrayList<>();
        for (StreamMessage<byte[], byte[]> message : messages) {
            Map<String, Object> fields = new LinkedHashMap<>();
            message.getBody().forEach((k, v) -> fields.put(KeyRecord.text(k, base64), KeyRecord.text(v, base64)));
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", message.getId());
            entry.put("fields", fields);
            entries.add(entry);
        }
        KeyRecord record = KeyRecord.of(key, base64, "stream", ttlMs, encoding, entries);
        record.lastGeneratedId = lastGeneratedId;
        if (includeConsumerGroups) {
            record.consumerGroups = fetchConsumerGroups(cmds, key);
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
            byte[] groupName = String.valueOf(nameObj).getBytes(StandardCharsets.UTF_8);
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
        return (o instanceof byte[] b) ? new String(b, StandardCharsets.UTF_8) : o;
    }

    private static List<KeyRecord> chunkMap(byte[] key, boolean base64, long ttlMs, String encoding, Map<String, Object> fields, int chunkSize) {
        if (fields.size() <= chunkSize) {
            return List.of(KeyRecord.of(key, base64, "hash", ttlMs, encoding, fields));
        }
        List<KeyRecord> chunks = new ArrayList<>();
        Map<String, Object> current = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            current.put(entry.getKey(), entry.getValue());
            if (current.size() == chunkSize) {
                chunks.add(KeyRecord.of(key, base64, "hash", ttlMs, encoding, current));
                current = new LinkedHashMap<>();
            }
        }
        if (!current.isEmpty()) {
            chunks.add(KeyRecord.of(key, base64, "hash", ttlMs, encoding, current));
        }
        return chunks;
    }

    private static List<KeyRecord> chunkList(byte[] key, boolean base64, String type, long ttlMs, String encoding, List<Object> items, int chunkSize) {
        if (items.size() <= chunkSize) {
            return List.of(KeyRecord.of(key, base64, type, ttlMs, encoding, items));
        }
        List<KeyRecord> chunks = new ArrayList<>();
        for (int i = 0; i < items.size(); i += chunkSize) {
            chunks.add(KeyRecord.of(key, base64, type, ttlMs, encoding, items.subList(i, Math.min(i + chunkSize, items.size()))));
        }
        return chunks;
    }
}
