package io.dragonfly.nifi.redis.util;

import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Claims scan partitions and checkpoints SCAN cursors via {@link DistributedMapCacheClient},
 * per spec section 4.1.1/4.1.3/7.3.
 *
 * <p>The spec calls for CAS semantics via a {@code replace(key, oldValue, newValue)} method,
 * and for claim/completion state tracked as {@code Set<partitionIndex>} values. NiFi 2.x's
 * {@code DistributedMapCacheClient} interface exposes neither a replace/CAS method nor
 * per-entry TTL - only {@code putIfAbsent}/{@code get}/{@code put}/{@code remove}. This class
 * gets equivalent claim safety from {@code putIfAbsent} (which is itself atomic - exactly one
 * caller ever succeeds for a given key) and avoids the shared-Set design entirely in favor of
 * one independent key per partition, since read-modify-write on a shared collection value
 * would not be safe with these primitives.
 *
 * <p>A claim's value is the time its owner last heartbeat it. A claim whose heartbeat is older than
 * the stale timeout belonged to a task that died (a killed NiFi node), and is taken over, but only
 * through an {@link AtomicDistributedMapCacheClient} compare-and-replace, so of two tasks that both
 * see it stale exactly one wins. A cache without that never takes a claim over: the claim stays
 * until it is cleared by hand or a new migration id is used.
 */
public class PartitionAssigner {

    private static final String COMPLETE_MARKER = "true";

    private static final Serializer<String> STRING_SERIALIZER =
            (value, out) -> out.write(value.getBytes(StandardCharsets.UTF_8));
    private static final Deserializer<String> STRING_DESERIALIZER =
            // NiFi's built-in DistributedMapCacheClient returns a zero-length (non-null) byte
            // array on a cache miss, not null - treat that the same as absent, or callers like
            // restoreCursor() see Optional.of("") instead of Optional.empty() and skip their
            // fallback logic.
            bytes -> (bytes == null || bytes.length == 0) ? null : new String(bytes, StandardCharsets.UTF_8);

    private final DistributedMapCacheClient cache;
    private final String migrationId;

    public PartitionAssigner(DistributedMapCacheClient cache, String migrationId) {
        this.cache = cache;
        this.migrationId = migrationId;
    }

    /** Attempts to claim the first unclaimed partition in [0, partitionCount), never taking one over. */
    public Optional<Integer> claimPartition(int partitionCount) throws IOException {
        return claimPartition(partitionCount, Long.MAX_VALUE);
    }

    /** Like {@link #claimPartition(int)}, but also takes over a claim not heartbeat for {@code staleAfterMs}. */
    public Optional<Integer> claimPartition(int partitionCount, long staleAfterMs) throws IOException {
        for (int i = 0; i < partitionCount; i++) {
            String now = String.valueOf(System.currentTimeMillis());
            if (cache.putIfAbsent(claimKey(i), now, STRING_SERIALIZER, STRING_SERIALIZER) || takeOverIfStale(i, staleAfterMs)) {
                return Optional.of(i);
            }
        }
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    private boolean takeOverIfStale(int partitionIndex, long staleAfterMs) throws IOException {
        if (!(cache instanceof AtomicDistributedMapCacheClient<?> atomic) || staleAfterMs == Long.MAX_VALUE) {
            return false;
        }
        AtomicCacheEntry<String, String, Object> entry = ((AtomicDistributedMapCacheClient<Object>) atomic)
                .fetch(claimKey(partitionIndex), STRING_SERIALIZER, STRING_DESERIALIZER);
        if (entry == null || entry.getValue() == null || System.currentTimeMillis() - heartbeatOf(entry.getValue()) < staleAfterMs) {
            return false;
        }
        entry.setValue(String.valueOf(System.currentTimeMillis()));
        return ((AtomicDistributedMapCacheClient<Object>) atomic).replace(entry, STRING_SERIALIZER, STRING_SERIALIZER);
    }

    /** A claim written before heartbeats existed holds the word "claimed", which reads as never heartbeat. */
    private static long heartbeatOf(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /** Keeps a claim fresh so other tasks do not take it over while its owner is still working. */
    public void heartbeat(int partitionIndex) throws IOException {
        cache.put(claimKey(partitionIndex), String.valueOf(System.currentTimeMillis()), STRING_SERIALIZER, STRING_SERIALIZER);
    }

    /** Releases a previously claimed partition so another task may claim it. */
    public void releasePartition(int partitionIndex) throws IOException {
        cache.remove(claimKey(partitionIndex), STRING_SERIALIZER);
    }

    public void checkpointCursor(int partitionIndex, String cursor) throws IOException {
        cache.put(cursorKey(partitionIndex), cursor, STRING_SERIALIZER, STRING_SERIALIZER);
    }

    public Optional<String> restoreCursor(int partitionIndex) throws IOException {
        return Optional.ofNullable(cache.get(cursorKey(partitionIndex), STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    /**
     * The cluster's topology when this migration's scan began, shared by every partition so they
     * divide the same shards between them whatever has moved since: the first to ask records
     * {@code proposed}, and everyone gets what was recorded.
     */
    public String baselineTopology(String proposed) throws IOException {
        cache.putIfAbsent(baselineKey(), proposed, STRING_SERIALIZER, STRING_SERIALIZER);
        return cache.get(baselineKey(), STRING_SERIALIZER, STRING_DESERIALIZER);
    }

    public void markPartitionComplete(int partitionIndex) throws IOException {
        cache.put(completeKey(partitionIndex), COMPLETE_MARKER, STRING_SERIALIZER, STRING_SERIALIZER);
    }

    public boolean isPartitionComplete(int partitionIndex) throws IOException {
        return COMPLETE_MARKER.equals(cache.get(completeKey(partitionIndex), STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    private String claimKey(int partitionIndex) {
        return "redis.migration.partition." + migrationId + ".claim." + partitionIndex;
    }

    private String cursorKey(int partitionIndex) {
        return "redis.migration.cursor." + migrationId + "." + partitionIndex;
    }

    private String baselineKey() {
        return "redis.migration.topology." + migrationId;
    }

    private String completeKey(int partitionIndex) {
        return "redis.migration.partition." + migrationId + ".complete." + partitionIndex;
    }
}
