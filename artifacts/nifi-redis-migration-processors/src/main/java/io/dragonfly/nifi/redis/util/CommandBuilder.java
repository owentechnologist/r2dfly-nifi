package io.dragonfly.nifi.redis.util;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.SetArgs;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Builds and dispatches the Dragonfly write command(s) for a {@link KeyRecord}, per spec
 * section 4.3.1 and Appendix B.
 *
 * <p>Stream writes reproduce entries with their original IDs via {@code XADD ... ID}, which
 * also advances the target stream's internal last-id the same way {@code XSETID} would for any
 * copied entry - so {@code XSETID} itself is not issued. The one case that differs is an empty
 * stream whose last-id is ahead of all (trimmed-away) entries; reproducing that exactly would
 * require a raw {@code XSETID} call that Lettuce does not expose as a typed method, so it is
 * treated as an accepted gap rather than reached for via low-level protocol dispatch.
 */
public final class CommandBuilder {

    public enum TtlStrategy { PRESERVE, STRIP, RESET }

    public enum ConflictStrategy { OVERWRITE, SKIP, FAIL }

    public enum WriteOutcome { SUCCESS, SKIPPED, CONFLICT }

    private CommandBuilder() {
    }

    /** Default cap on elements/fields/members sent in a single RPUSH/SADD/ZADD/HSET call - a
     * key with more than this many entries is written across several smaller commands instead
     * of one command whose argument count scales with the key's own size. Protects the target
     * from a single oversized command (e.g. a 10-million-member set becoming one 10,000,002-
     * argument SADD) regardless of how the record was chunked (or not) upstream. */
    public static final int DEFAULT_WRITE_CHUNK_SIZE = 1000;

    public static CompletableFuture<WriteOutcome> write(
            RedisClusterAsyncCommands<byte[], byte[]> target,
            KeyRecord record,
            String keyPrefix,
            String keyPrefixSeparator,
            TtlStrategy ttlStrategy,
            long ttlResetOffsetMs,
            ConflictStrategy conflictStrategy) {
        return write(target, record, keyPrefix, keyPrefixSeparator, ttlStrategy, ttlResetOffsetMs,
                conflictStrategy, DEFAULT_WRITE_CHUNK_SIZE);
    }

    public static CompletableFuture<WriteOutcome> write(
            RedisClusterAsyncCommands<byte[], byte[]> target,
            KeyRecord record,
            String keyPrefix,
            String keyPrefixSeparator,
            TtlStrategy ttlStrategy,
            long ttlResetOffsetMs,
            ConflictStrategy conflictStrategy,
            int writeChunkSize) {

        byte[] targetKey = prefixedKey(record.key, keyPrefix, keyPrefixSeparator);
        Long effectiveTtl = effectiveTtl(record.ttlMs, ttlStrategy, ttlResetOffsetMs);

        if (record.dumpPayload != null) {
            return writeDump(target, targetKey, record.dumpPayload, effectiveTtl, conflictStrategy);
        }

        if (conflictStrategy == ConflictStrategy.OVERWRITE) {
            return dispatchWrite(target, record, targetKey, effectiveTtl, writeChunkSize).thenApply(v -> WriteOutcome.SUCCESS);
        }
        return toCf(target.exists(targetKey)).thenCompose(count -> {
            if (count != null && count > 0) {
                WriteOutcome outcome = conflictStrategy == ConflictStrategy.SKIP ? WriteOutcome.SKIPPED : WriteOutcome.CONFLICT;
                return CompletableFuture.completedFuture(outcome);
            }
            return dispatchWrite(target, record, targetKey, effectiveTtl, writeChunkSize).thenApply(v -> WriteOutcome.SUCCESS);
        });
    }

    private static CompletableFuture<Void> dispatchWrite(
            RedisClusterAsyncCommands<byte[], byte[]> target, KeyRecord record, byte[] key, Long ttlMs, int writeChunkSize) {
        return switch (record.type) {
            case "string" -> writeString(target, key, record, ttlMs);
            case "hash" -> writeHash(target, key, record, ttlMs, writeChunkSize);
            case "list" -> writeList(target, key, record, ttlMs, writeChunkSize);
            case "set" -> writeSet(target, key, record, ttlMs, writeChunkSize);
            case "zset" -> writeZset(target, key, record, ttlMs, writeChunkSize);
            case "stream" -> writeStream(target, key, record, ttlMs);
            default -> CompletableFuture.failedFuture(new IllegalArgumentException("Unsupported type: " + record.type));
        };
    }

    /** --dfly-to-dfly path: a RESTORE's argument count is fixed (key, ttl, one payload bulk
     * string, flags) regardless of how many elements the source key held - unlike ZADD/SADD/
     * RPUSH/HSET, whose argument count scales with the key's own size (the exact defect that
     * turned a 5-million-member zset into one 10,000,002-argument command). The payload's own
     * byte length is still bounded by the target's max-bulk-string-length limit, a different,
     * much larger limit than the multibulk-element-count one this sidesteps entirely - callers
     * (RedisTypeDeserializer's Max Dump Payload Bytes) are expected to guard against that
     * separately before a payload this large ever reaches here. */
    private static CompletableFuture<WriteOutcome> writeDump(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, byte[] payload, Long ttlMs,
            ConflictStrategy conflictStrategy) {
        if (conflictStrategy == ConflictStrategy.OVERWRITE) {
            return dispatchRestore(target, key, payload, ttlMs).thenApply(v -> WriteOutcome.SUCCESS);
        }
        return toCf(target.exists(key)).thenCompose(count -> {
            if (count != null && count > 0) {
                WriteOutcome outcome = conflictStrategy == ConflictStrategy.SKIP ? WriteOutcome.SKIPPED : WriteOutcome.CONFLICT;
                return CompletableFuture.completedFuture(outcome);
            }
            return dispatchRestore(target, key, payload, ttlMs).thenApply(v -> WriteOutcome.SUCCESS);
        });
    }

    private static CompletableFuture<Void> dispatchRestore(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, byte[] payload, Long ttlMs) {
        RestoreArgs args = new RestoreArgs().replace().ttl(ttlMs != null ? ttlMs : 0);
        return toCf(target.restore(key, payload, args)).thenApply(v -> null);
    }

    private static CompletableFuture<Void> writeString(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, KeyRecord record, Long ttlMs) {
        byte[] value = String.valueOf(record.value).getBytes(StandardCharsets.UTF_8);
        RedisFuture<String> future = (ttlMs != null)
                ? target.set(key, value, new SetArgs().px(ttlMs))
                : target.set(key, value);
        return toCf(future).thenApply(v -> null);
    }

    private static CompletableFuture<Void> writeHash(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, KeyRecord record, Long ttlMs, int writeChunkSize) {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        Map<byte[], byte[]> chunk = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : asMap(record.value).entrySet()) {
            chunk.put(entry.getKey().getBytes(StandardCharsets.UTF_8), String.valueOf(entry.getValue()).getBytes(StandardCharsets.UTF_8));
            if (chunk.size() == writeChunkSize) {
                futures.add(toCf(target.hset(key, chunk)));
                chunk = new LinkedHashMap<>();
            }
        }
        if (!chunk.isEmpty()) {
            futures.add(toCf(target.hset(key, chunk)));
        }
        return allOf(futures).thenCompose(v -> applyTtl(target, key, ttlMs));
    }

    private static CompletableFuture<Void> writeList(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, KeyRecord record, Long ttlMs, int writeChunkSize) {
        byte[][] values = toByteArrays(asList(record.value));
        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(toCf(target.del(key)));
        for (byte[][] part : chunkArray(values, writeChunkSize)) {
            futures.add(toCf(target.rpush(key, part)));
        }
        return allOf(futures).thenCompose(v -> applyTtl(target, key, ttlMs));
    }

    private static CompletableFuture<Void> writeSet(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, KeyRecord record, Long ttlMs, int writeChunkSize) {
        byte[][] values = toByteArrays(asList(record.value));
        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(toCf(target.del(key)));
        for (byte[][] part : chunkArray(values, writeChunkSize)) {
            futures.add(toCf(target.sadd(key, part)));
        }
        return allOf(futures).thenCompose(v -> applyTtl(target, key, ttlMs));
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Void> writeZset(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, KeyRecord record, Long ttlMs, int writeChunkSize) {
        List<Object> items = asList(record.value);
        ScoredValue<byte[]>[] scored = new ScoredValue[items.size()];
        for (int i = 0; i < items.size(); i++) {
            Map<String, Object> entry = (Map<String, Object>) items.get(i);
            double score = ((Number) entry.get("score")).doubleValue();
            byte[] member = String.valueOf(entry.get("member")).getBytes(StandardCharsets.UTF_8);
            scored[i] = ScoredValue.just(score, member);
        }
        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(toCf(target.del(key)));
        for (ScoredValue<byte[]>[] part : chunkArray(scored, writeChunkSize)) {
            futures.add(toCf(target.zadd(key, part)));
        }
        return allOf(futures).thenCompose(v -> applyTtl(target, key, ttlMs));
    }

    /** Dispatches every chunk (and, for list/set/zset, the leading DEL) as concurrent pipelined
     * commands on the same connection rather than chaining them sequentially - Lettuce writes
     * commands to the wire in call order regardless of when each completes, so ordering (DEL
     * before any chunk; chunk N before chunk N+1 for list) is preserved without waiting for each
     * one's reply before sending the next. A key with thousands of chunks would otherwise pay a
     * full network round trip per chunk serially - against a remote target that turns a
     * multi-million-element key into a multi-minute write instead of one bounded by throughput,
     * and risks the borrowed connection being returned to the pool (by the caller's own timeout)
     * while still-chained commands are in flight on it. */
    private static CompletableFuture<Void> allOf(List<CompletableFuture<?>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private static <T> List<T[]> chunkArray(T[] values, int chunkSize) {
        List<T[]> chunks = new ArrayList<>();
        for (int i = 0; i < values.length; i += chunkSize) {
            chunks.add(Arrays.copyOfRange(values, i, Math.min(i + chunkSize, values.length)));
        }
        return chunks;
    }

    /** Pipelined like {@link #writeList}/{@link #writeSet}/{@link #writeZset} (see {@link
     * #allOf}) rather than chained through {@code thenCompose} - a stream with many entries
     * would otherwise pay a network round trip per XADD. Entry order and the DEL-before-XADD-
     * before-XGROUP-CREATE ordering are still preserved because Lettuce writes commands to the
     * wire in call order, not completion order. */
    @SuppressWarnings("unchecked")
    private static CompletableFuture<Void> writeStream(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, KeyRecord record, Long ttlMs) {
        List<Object> entries = asList(record.value);
        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(toCf(target.del(key)));
        for (Object o : entries) {
            Map<String, Object> entry = (Map<String, Object>) o;
            String id = String.valueOf(entry.get("id"));
            Map<String, Object> fieldsRaw = (Map<String, Object>) entry.get("fields");
            Map<byte[], byte[]> fields = new LinkedHashMap<>();
            fieldsRaw.forEach((k, v) -> fields.put(k.getBytes(StandardCharsets.UTF_8), String.valueOf(v).getBytes(StandardCharsets.UTF_8)));
            futures.add(toCf(target.xadd(key, new XAddArgs().id(id), fields)));
        }
        addConsumerGroupFutures(target, key, record, futures);
        return allOf(futures).thenCompose(v -> applyTtl(target, key, ttlMs));
    }

    @SuppressWarnings("unchecked")
    private static void addConsumerGroupFutures(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, KeyRecord record, List<CompletableFuture<?>> futures) {
        if (record.consumerGroups == null) {
            return;
        }
        for (Object o : (List<Object>) record.consumerGroups) {
            Map<String, Object> group = (Map<String, Object>) o;
            Object nameObj = group.get("name");
            Object lastDeliveredObj = group.getOrDefault("last-delivered-id", "0");
            if (nameObj == null) {
                continue;
            }
            byte[] groupName = String.valueOf(nameObj).getBytes(StandardCharsets.UTF_8);
            String lastDeliveredId = String.valueOf(lastDeliveredObj);
            XReadArgs.StreamOffset<byte[]> offset = XReadArgs.StreamOffset.from(key, lastDeliveredId);
            futures.add(toCf(target.xgroupCreate(offset, groupName, new XGroupCreateArgs().mkstream(true))));
        }
    }

    private static CompletableFuture<Void> applyTtl(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, Long ttlMs) {
        if (ttlMs == null) {
            return CompletableFuture.completedFuture(null);
        }
        return toCf(target.pexpire(key, ttlMs)).thenApply(v -> null);
    }

    private static byte[] prefixedKey(String key, String prefix, String separator) {
        if (prefix == null || prefix.isEmpty()) {
            return key.getBytes(StandardCharsets.UTF_8);
        }
        return (prefix + separator + key).getBytes(StandardCharsets.UTF_8);
    }

    private static Long effectiveTtl(long sourceTtlMs, TtlStrategy strategy, long resetOffsetMs) {
        return switch (strategy) {
            case STRIP -> null;
            case PRESERVE -> sourceTtlMs > 0 ? sourceTtlMs : null;
            case RESET -> sourceTtlMs > 0 ? sourceTtlMs + resetOffsetMs : null;
        };
    }

    private static byte[][] toByteArrays(List<Object> items) {
        byte[][] values = new byte[items.size()][];
        for (int i = 0; i < items.size(); i++) {
            values[i] = String.valueOf(items.get(i)).getBytes(StandardCharsets.UTF_8);
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object value) {
        return (List<Object>) value;
    }

    private static <T> CompletableFuture<T> toCf(RedisFuture<T> future) {
        return future.toCompletableFuture();
    }
}
