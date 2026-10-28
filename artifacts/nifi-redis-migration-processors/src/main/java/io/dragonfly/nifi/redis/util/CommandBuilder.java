package io.dragonfly.nifi.redis.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.XClaimArgs;
import io.lettuce.core.SetArgs;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.IntegerListOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Builds and dispatches the Dragonfly write command(s) for a {@link KeyRecord}, per spec
 * section 4.3.1 and Appendix B.
 *
 * <p>Stream writes reproduce entries with their original IDs, then each group at its
 * last-delivered-id and entries-read, then the source's last-generated-id, then each consumer and
 * each pending entry with its owner, delivery time and delivery count. Dragonfly 2.0.2 rejects
 * XSETID's ENTRIESADDED and MAXDELETEDID, so those two counters (they only feed XINFO's lag) are
 * whatever the copied entries produce. Its XCLAIM IDLE is wrong (the entry reads back as idle
 * since 1970), so the delivery time goes out as XCLAIM TIME on the target's own clock, read with
 * TIME first: delivered_at_ms is on the reading host's clock, and server clocks drift from it.
 */
public final class CommandBuilder {

    public enum TtlStrategy { PRESERVE, STRIP, RESET }

    public enum ConflictStrategy { OVERWRITE, SKIP, FAIL }

    public enum WriteOutcome { SUCCESS, SKIPPED, CONFLICT, RESTORE_REJECTED }

    private CommandBuilder() {
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Commands Lettuce 6.3 has no typed method for. */
    private enum RawCommand implements ProtocolKeyword {
        XSETID, XGROUP, HEXPIRE, TIME;

        @Override
        public byte[] getBytes() {
            return name().getBytes(StandardCharsets.US_ASCII);
        }
    }

    /** Default cap on elements/fields/members sent in a single RPUSH/SADD/ZADD/HSET call - a
     * key with more than this many entries is written across several smaller commands instead
     * of one command whose argument count scales with the key's own size. Protects the target
     * from a single oversized command (e.g. a 10-million-member set becoming one 10,000,002-
     * argument SADD) regardless of how the record was chunked (or not) upstream. */
    public static final int DEFAULT_WRITE_CHUNK_SIZE = 1000;

    public static CompletableFuture<WriteOutcome> write(
            RedisClusterAsyncCommands<byte[], byte[]> target,
            StatefulConnection<byte[], byte[]> rawConn,
            KeyRecord record,
            String targetKey,
            TtlStrategy ttlStrategy,
            long ttlResetOffsetMs,
            ConflictStrategy conflictStrategy) {
        return write(target, rawConn, record, targetKey, ttlStrategy, ttlResetOffsetMs,
                conflictStrategy, DEFAULT_WRITE_CHUNK_SIZE);
    }

    public static CompletableFuture<WriteOutcome> write(
            RedisClusterAsyncCommands<byte[], byte[]> target,
            StatefulConnection<byte[], byte[]> rawConn,
            KeyRecord record,
            String targetKeyName,
            TtlStrategy ttlStrategy,
            long ttlResetOffsetMs,
            ConflictStrategy conflictStrategy,
            int writeChunkSize) {
        // A record that cannot even be turned into commands fails as its own write, not the caller's batch.
        try {
            return dispatch(target, rawConn, record, targetKeyName, ttlStrategy, ttlResetOffsetMs, conflictStrategy, writeChunkSize);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static CompletableFuture<WriteOutcome> dispatch(
            RedisClusterAsyncCommands<byte[], byte[]> target,
            StatefulConnection<byte[], byte[]> rawConn,
            KeyRecord record,
            String targetKeyName,
            TtlStrategy ttlStrategy,
            long ttlResetOffsetMs,
            ConflictStrategy conflictStrategy,
            int writeChunkSize) {
        byte[] targetKey = targetKeyName.getBytes(StandardCharsets.UTF_8);
        Long effectiveTtl = effectiveTtl(record.ttlMs, ttlStrategy, ttlResetOffsetMs);
        boolean keepFieldTtls = ttlStrategy != TtlStrategy.STRIP;

        if (record.dumpPayload != null) {
            return writeDump(target, targetKey, record.dumpPayload, effectiveTtl, conflictStrategy);
        }

        if (conflictStrategy == ConflictStrategy.OVERWRITE) {
            return dispatchWrite(target, rawConn, record, targetKey, effectiveTtl, keepFieldTtls, writeChunkSize).thenApply(v -> WriteOutcome.SUCCESS);
        }
        return toCf(target.exists(targetKey)).thenCompose(count -> {
            if (count != null && count > 0) {
                WriteOutcome outcome = conflictStrategy == ConflictStrategy.SKIP ? WriteOutcome.SKIPPED : WriteOutcome.CONFLICT;
                return CompletableFuture.completedFuture(outcome);
            }
            return dispatchWrite(target, rawConn, record, targetKey, effectiveTtl, keepFieldTtls, writeChunkSize).thenApply(v -> WriteOutcome.SUCCESS);
        });
    }

    private static CompletableFuture<Void> dispatchWrite(
            RedisClusterAsyncCommands<byte[], byte[]> target, StatefulConnection<byte[], byte[]> rawConn,
            KeyRecord record, byte[] key, Long ttlMs, boolean keepFieldTtls, int writeChunkSize) {
        return switch (record.type) {
            case "string" -> writeString(target, key, record, ttlMs);
            case "hash" -> writeHash(target, key, record, ttlMs, keepFieldTtls, writeChunkSize);
            case "list" -> writeList(target, key, record, ttlMs, writeChunkSize);
            case "set" -> writeSet(target, key, record, ttlMs, writeChunkSize);
            case "zset" -> writeZset(target, key, record, ttlMs, writeChunkSize);
            case "stream" -> writeStream(target, key, record, ttlMs);
            case "json" -> writeJson(target, rawConn, key, record, ttlMs);
            case KeyRecord.DELETED -> toCf(target.del(key)).thenApply(v -> null);
            default -> CompletableFuture.failedFuture(new IllegalArgumentException("Unsupported type: " + record.type));
        };
    }

    /** No leading DEL, unlike {@link #writeHash}/{@link #writeList}/{@link #writeSet}/
     * {@link #writeZset} - {@code JSON.SET key . <value>} already replaces the whole document,
     * so a DEL first would be redundant work rather than a correctness requirement. */
    private static CompletableFuture<Void> writeJson(
            RedisClusterAsyncCommands<byte[], byte[]> target, StatefulConnection<byte[], byte[]> rawConn,
            byte[] key, KeyRecord record, Long ttlMs) {
        byte[] json;
        try {
            json = OBJECT_MAPPER.writeValueAsBytes(record.value);
        } catch (java.io.IOException e) {
            return CompletableFuture.failedFuture(new UncheckedIOException(
                    "Failed to serialize JSON value for key " + new String(key, StandardCharsets.UTF_8), e));
        }
        return RawModuleCommands.jsonSet(rawConn, key, json).thenCompose(v -> applyTtl(target, key, ttlMs));
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
            return dispatchRestore(target, key, payload, ttlMs);
        }
        return toCf(target.exists(key)).thenCompose(count -> {
            if (count != null && count > 0) {
                WriteOutcome outcome = conflictStrategy == ConflictStrategy.SKIP ? WriteOutcome.SKIPPED : WriteOutcome.CONFLICT;
                return CompletableFuture.completedFuture(outcome);
            }
            return dispatchRestore(target, key, payload, ttlMs);
        });
    }

    /** A server error reply means the target cannot read this payload (for example a DUMP version
     * newer than it supports); that is permanent for the key, so it is reported as an outcome the
     * caller can re-route to the typed path instead of an exception that would only be retried. */
    private static CompletableFuture<WriteOutcome> dispatchRestore(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, byte[] payload, Long ttlMs) {
        RestoreArgs args = new RestoreArgs().replace().ttl(ttlMs != null ? ttlMs : 0);
        return toCf(target.restore(key, payload, args))
                .thenApply(v -> WriteOutcome.SUCCESS)
                .exceptionallyCompose(e -> {
                    Throwable cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
                    return cause instanceof RedisCommandExecutionException && !transientReply(cause.getMessage())
                            ? CompletableFuture.completedFuture(WriteOutcome.RESTORE_REJECTED)
                            : CompletableFuture.failedFuture(cause);
                });
    }

    /** Replies about the target's state, not the payload: the write is retried rather than given up on. */
    private static final List<String> TRANSIENT_REPLIES = List.of("OOM", "LOADING", "BUSY", "READONLY", "TRYAGAIN",
            "CLUSTERDOWN", "MASTERDOWN", "NOREPLICAS", "MOVED", "ASK");

    private static boolean transientReply(String message) {
        return message != null && TRANSIENT_REPLIES.stream().anyMatch(prefix -> message.startsWith(prefix + " ") || message.equals(prefix));
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
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, KeyRecord record, Long ttlMs, boolean keepFieldTtls,
            int writeChunkSize) {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(toCf(target.del(key)));
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
        if (keepFieldTtls && record.fieldExpireAtMs != null) {
            addFieldTtlFutures(target, key, record.fieldExpireAtMs, futures);
        }
        return allOf(futures).thenCompose(v -> applyTtl(target, key, ttlMs));
    }

    /** HEXPIRE takes seconds and is the only relative form Dragonfly 2.0.2 has (no HPEXPIRE), so each
     * TTL rounds up to the next second; fields sharing a value go out in one call. A field whose
     * expiry passed while the record was in flight is deleted, as the source will have done. */
    private static void addFieldTtlFutures(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, Map<String, Long> expireAtMs, List<CompletableFuture<?>> futures) {
        long now = System.currentTimeMillis();
        Map<Long, List<byte[]>> fieldsBySeconds = new java.util.TreeMap<>();
        List<byte[]> expired = new ArrayList<>();
        expireAtMs.forEach((field, at) -> {
            byte[] raw = field.getBytes(StandardCharsets.UTF_8);
            long remainingMs = at - now;
            if (remainingMs <= 0) {
                expired.add(raw);
            } else {
                fieldsBySeconds.computeIfAbsent((remainingMs + 999) / 1000, s -> new ArrayList<>()).add(raw);
            }
        });
        if (!expired.isEmpty()) {
            futures.add(toCf(target.hdel(key, expired.toArray(new byte[0][]))));
        }
        fieldsBySeconds.forEach((seconds, fields) -> {
            CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addKey(key).add(seconds).add("FIELDS").add(fields.size());
            fields.forEach(args::add);
            futures.add(toCf(target.dispatch(RawCommand.HEXPIRE, new IntegerListOutput<>(ByteArrayCodec.INSTANCE), args)));
        });
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
            double score = score(entry.get("score"));
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

    /** Jackson writes an infinite score as the string "Infinity" or "-Infinity". */
    private static double score(Object value) {
        return value instanceof Number n ? n.doubleValue() : Double.parseDouble(String.valueOf(value));
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
        String lastEntryId = null;
        for (Object o : entries) {
            Map<String, Object> entry = (Map<String, Object>) o;
            String id = String.valueOf(entry.get("id"));
            Map<String, Object> fieldsRaw = (Map<String, Object>) entry.get("fields");
            Map<byte[], byte[]> fields = new LinkedHashMap<>();
            fieldsRaw.forEach((k, v) -> fields.put(k.getBytes(StandardCharsets.UTF_8), String.valueOf(v).getBytes(StandardCharsets.UTF_8)));
            futures.add(toCf(target.xadd(key, new XAddArgs().id(id), fields)));
            lastEntryId = id;
        }
        List<Map<String, Object>> groups = record.consumerGroups == null ? List.of()
                : ((List<Object>) record.consumerGroups).stream().map(g -> (Map<String, Object>) g).filter(g -> g.get("name") != null).toList();
        for (Map<String, Object> group : groups) {
            String lastDeliveredId = String.valueOf(group.getOrDefault("last-delivered-id", "0"));
            XReadArgs.StreamOffset<byte[]> offset = XReadArgs.StreamOffset.from(key, lastDeliveredId);
            futures.add(toCf(target.xgroupCreate(offset, bytes(group.get("name")), new XGroupCreateArgs().mkstream(true))));
            // Through SETID rather than CREATE's own ENTRIESREAD, which Dragonfly 2.0.2 accepts and ignores.
            if (group.get("entries-read") instanceof Number entriesRead) {
                futures.add(toCf(target.dispatch(RawCommand.XGROUP, new StatusOutput<>(ByteArrayCodec.INSTANCE),
                        new CommandArgs<>(ByteArrayCodec.INSTANCE).add("SETID").addKey(key).add(bytes(group.get("name")))
                                .add(lastDeliveredId).add("ENTRIESREAD").add(entriesRead.longValue()))));
            }
        }
        // After the groups: a stream with no entries left exists on the target only once MKSTREAM
        // has created it, and XSETID needs an existing key.
        if (record.lastGeneratedId != null && !record.lastGeneratedId.equals(lastEntryId) && (lastEntryId != null || !groups.isEmpty())) {
            futures.add(toCf(target.dispatch(RawCommand.XSETID, new StatusOutput<>(ByteArrayCodec.INSTANCE),
                    new CommandArgs<>(ByteArrayCodec.INSTANCE).addKey(key).add(record.lastGeneratedId))));
        }
        boolean anyPending = false;
        for (Map<String, Object> group : groups) {
            byte[] groupName = bytes(group.get("name"));
            if (group.get("consumers") instanceof List<?> consumers) {
                for (Object c : consumers) {
                    if (c instanceof Map<?, ?> consumer && consumer.get("name") != null) {
                        futures.add(toCf(target.xgroupCreateconsumer(key, io.lettuce.core.Consumer.from(groupName, bytes(consumer.get("name"))))));
                    }
                }
            }
            anyPending |= group.get("pending") instanceof List<?> pending && !pending.isEmpty();
        }
        CompletableFuture<Void> written = allOf(futures);
        if (anyPending) {
            // Raw, because Dragonfly answers TIME with integers where Redis sends bulk strings, which
            // Lettuce's typed time() cannot parse.
            written = written.thenCompose(v -> toCf(target.dispatch(RawCommand.TIME, new ArrayOutput<>(ByteArrayCodec.INSTANCE), new CommandArgs<>(ByteArrayCodec.INSTANCE))))
                    .thenCompose(time -> claimPending(target, key, groups, targetClockOffsetMs(time)));
        }
        return written.thenCompose(v -> applyTtl(target, key, ttlMs));
    }

    /** TIME answers seconds and microseconds; the offset maps this host's clock onto the target's. */
    private static long targetClockOffsetMs(List<Object> time) {
        long targetNowMs = asLong(time.get(0)) * 1000 + asLong(time.get(1)) / 1000;
        return targetNowMs - System.currentTimeMillis();
    }

    private static long asLong(Object reply) {
        return reply instanceof Number n ? n.longValue() : Long.parseLong(new String((byte[]) reply, StandardCharsets.US_ASCII));
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Void> claimPending(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, List<Map<String, Object>> groups, long clockOffsetMs) {
        List<CompletableFuture<?>> claims = new ArrayList<>();
        for (Map<String, Object> group : groups) {
            if (group.get("pending") instanceof List<?> pending) {
                for (Object p : pending) {
                    if (p instanceof Map<?, ?> entry) {
                        claims.add(toCf(target.xclaim(key, io.lettuce.core.Consumer.from(bytes(group.get("name")), bytes(entry.get("consumer"))),
                                claimArgs((Map<String, Object>) entry, clockOffsetMs), String.valueOf(entry.get("id")))));
                    }
                }
            }
        }
        return allOf(claims);
    }

    /** Records written before delivered_at_ms existed only carry the idle time as of their read. */
    private static XClaimArgs claimArgs(Map<String, Object> pending, long clockOffsetMs) {
        long deliveredAt = pending.get("delivered_at_ms") instanceof Number at ? at.longValue()
                : System.currentTimeMillis() - ((Number) pending.getOrDefault("ms_since_last_delivery", 0L)).longValue();
        long deliveries = ((Number) pending.getOrDefault("redelivery_count", 1L)).longValue();
        return new XClaimArgs().minIdleTime(0).time(deliveredAt + clockOffsetMs).retryCount(deliveries).force().justid();
    }

    private static byte[] bytes(Object value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    private static CompletableFuture<Void> applyTtl(
            RedisClusterAsyncCommands<byte[], byte[]> target, byte[] key, Long ttlMs) {
        if (ttlMs == null) {
            return CompletableFuture.completedFuture(null);
        }
        return toCf(target.pexpire(key, ttlMs)).thenApply(v -> null);
    }

    /** The TTL a key gets on the target, or null for none. */
    public static Long effectiveTtl(long sourceTtlMs, TtlStrategy strategy, long resetOffsetMs) {
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
