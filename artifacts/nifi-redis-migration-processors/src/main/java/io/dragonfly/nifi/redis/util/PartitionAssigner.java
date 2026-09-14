package io.dragonfly.nifi.redis.util;

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
 * would not be safe with these primitives. The one real gap versus the spec's design: without
 * TTL, a claim left behind by an ungracefully-killed NiFi node is not automatically released.
 * Operators can clear the specific claim key by hand, or start a fresh {@code migration.id}.
 */
public class PartitionAssigner {

    private static final String CLAIMED_MARKER = "claimed";
    private static final String COMPLETE_MARKER = "true";

    private static final Serializer<String> STRING_SERIALIZER =
            (value, out) -> out.write(value.getBytes(StandardCharsets.UTF_8));
    private static final Deserializer<String> STRING_DESERIALIZER =
            // NiFi's built-in DistributedMapCacheClient returns a zero-length (non-null) byte
            // array on a cache miss, not null - treat that the same as absent, or callers like
            // restoreCursor() see Optional.of("") instead of Optional.empty() and skip their
            // fallback logic.
            bytes -> (bytes == null || bytes.length == 0) ? null : new String(bytes, StandardCharsets.UTF_8);

    private static final int[] CRC16_TABLE = buildCrc16Table();
    private static final int REDIS_CLUSTER_SLOTS = 16384;

    private final DistributedMapCacheClient cache;
    private final String migrationId;

    public PartitionAssigner(DistributedMapCacheClient cache, String migrationId) {
        this.cache = cache;
        this.migrationId = migrationId;
    }

    /** Attempts to claim the first unclaimed partition in [0, partitionCount). */
    public Optional<Integer> claimPartition(int partitionCount) throws IOException {
        for (int i = 0; i < partitionCount; i++) {
            if (cache.putIfAbsent(claimKey(i), CLAIMED_MARKER, STRING_SERIALIZER, STRING_SERIALIZER)) {
                return Optional.of(i);
            }
        }
        return Optional.empty();
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

    private String completeKey(int partitionIndex) {
        return "redis.migration.partition." + migrationId + ".complete." + partitionIndex;
    }

    /**
     * Whether a standalone-mode task owning {@code partitionIndex} (of {@code partitionCount})
     * should process {@code key}, per spec 4.1.1's CRC16-modulo fallback for topologies where
     * SCAN can't be range-partitioned server-side.
     */
    public static boolean standaloneOwnsKey(byte[] key, int partitionIndex, int partitionCount) {
        return Math.floorMod(crc16(key), partitionCount) == partitionIndex;
    }

    /** Redis Cluster hash slot for a key, honoring {curly-brace} hash tags. */
    public static int hashSlot(byte[] key) {
        byte[] effective = hashTagOrWholeKey(key);
        return crc16(effective) % REDIS_CLUSTER_SLOTS;
    }

    /** [start, end) hash-slot range owned by {@code partitionIndex} of {@code partitionCount} in cluster mode. */
    public static int[] clusterSlotRange(int partitionIndex, int partitionCount) {
        int slotsPerPartition = REDIS_CLUSTER_SLOTS / partitionCount;
        int start = partitionIndex * slotsPerPartition;
        int end = (partitionIndex == partitionCount - 1) ? REDIS_CLUSTER_SLOTS : start + slotsPerPartition;
        return new int[] {start, end};
    }

    private static byte[] hashTagOrWholeKey(byte[] key) {
        int open = indexOf(key, (byte) '{', 0);
        if (open < 0) {
            return key;
        }
        int close = indexOf(key, (byte) '}', open + 1);
        if (close < 0 || close == open + 1) {
            return key;
        }
        int length = close - open - 1;
        byte[] tag = new byte[length];
        System.arraycopy(key, open + 1, tag, 0, length);
        return tag;
    }

    private static int indexOf(byte[] data, byte target, int fromIndex) {
        for (int i = fromIndex; i < data.length; i++) {
            if (data[i] == target) {
                return i;
            }
        }
        return -1;
    }

    private static int crc16(byte[] data) {
        int crc = 0;
        for (byte b : data) {
            crc = ((crc << 8) ^ CRC16_TABLE[((crc >> 8) ^ (b & 0xFF)) & 0xFF]) & 0xFFFF;
        }
        return crc;
    }

    private static int[] buildCrc16Table() {
        int[] table = new int[256];
        for (int i = 0; i < 256; i++) {
            int crc = i << 8;
            for (int j = 0; j < 8; j++) {
                crc = ((crc & 0x8000) != 0) ? ((crc << 1) ^ 0x1021) : (crc << 1);
            }
            table[i] = crc & 0xFFFF;
        }
        return table;
    }
}
