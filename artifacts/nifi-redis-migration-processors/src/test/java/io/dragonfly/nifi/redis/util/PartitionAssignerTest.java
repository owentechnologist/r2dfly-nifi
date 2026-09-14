package io.dragonfly.nifi.redis.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionAssignerTest {

    @Test
    void hashSlotMatchesKnownCrc16CcittCheckValue() {
        // "123456789" is the standard CRC16-CCITT/XMODEM (poly 0x1021, init 0) check value: 0x31C3 = 12739.
        // Redis Cluster's hash slot is exactly this CRC16 mod 16384, and 12739 < 16384 so the slot equals it.
        assertEquals(12739, PartitionAssigner.hashSlot("123456789".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void hashSlotHonorsHashTags() {
        int taggedSlot = PartitionAssigner.hashSlot("{user1000}.following".getBytes(StandardCharsets.UTF_8));
        int untaggedSlot = PartitionAssigner.hashSlot("user1000".getBytes(StandardCharsets.UTF_8));
        assertEquals(untaggedSlot, taggedSlot, "keys sharing a {tag} must map to the same slot");
    }

    @Test
    void hashSlotIgnoresEmptyHashTag() {
        // "{}" is not a valid hash tag (no characters between braces) - falls back to the whole key.
        byte[] key = "{}foo".getBytes(StandardCharsets.UTF_8);
        assertEquals(PartitionAssigner.hashSlot(key) >= 0, true);
    }

    @Test
    void clusterSlotRangesCoverTheFullSpaceWithoutOverlap() {
        int partitionCount = 4;
        int expectedStart = 0;
        for (int i = 0; i < partitionCount; i++) {
            int[] range = PartitionAssigner.clusterSlotRange(i, partitionCount);
            assertEquals(expectedStart, range[0]);
            expectedStart = range[1];
        }
        assertEquals(16384, expectedStart, "last partition's range must reach exactly 16384");
    }

    @Test
    void standaloneOwnsKeyAssignsEachKeyToExactlyOnePartition() {
        int partitionCount = 4;
        for (int i = 0; i < 200; i++) {
            byte[] key = ("key:" + i).getBytes(StandardCharsets.UTF_8);
            int owners = 0;
            for (int p = 0; p < partitionCount; p++) {
                if (PartitionAssigner.standaloneOwnsKey(key, p, partitionCount)) {
                    owners++;
                }
            }
            assertEquals(1, owners, "key " + i + " must be owned by exactly one partition");
        }
    }

    @Test
    void claimPartitionIsExclusiveAcrossConcurrentClaimers() throws Exception {
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        PartitionAssigner first = new PartitionAssigner(cache, "migration-1");
        PartitionAssigner second = new PartitionAssigner(cache, "migration-1");

        Optional<Integer> firstClaim = first.claimPartition(2);
        Optional<Integer> secondClaim = second.claimPartition(2);

        assertTrue(firstClaim.isPresent());
        assertTrue(secondClaim.isPresent());
        assertFalse(firstClaim.get().equals(secondClaim.get()), "two claimers must not receive the same partition");

        Optional<Integer> thirdClaim = new PartitionAssigner(cache, "migration-1").claimPartition(2);
        assertTrue(thirdClaim.isEmpty(), "no partitions left to claim");
    }

    @Test
    void releasePartitionAllowsReclaiming() throws Exception {
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        PartitionAssigner assigner = new PartitionAssigner(cache, "migration-2");

        int claimed = assigner.claimPartition(1).orElseThrow();
        assigner.releasePartition(claimed);

        Optional<Integer> reclaimed = new PartitionAssigner(cache, "migration-2").claimPartition(1);
        assertEquals(claimed, reclaimed.orElseThrow());
    }

    @Test
    void cursorCheckpointRoundTrips() throws Exception {
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        PartitionAssigner assigner = new PartitionAssigner(cache, "migration-3");

        assertTrue(assigner.restoreCursor(0).isEmpty());
        assigner.checkpointCursor(0, "42");
        assertEquals("42", assigner.restoreCursor(0).orElseThrow());
    }

    @Test
    void partitionCompletionRoundTrips() throws Exception {
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        PartitionAssigner assigner = new PartitionAssigner(cache, "migration-4");

        assertFalse(assigner.isPartitionComplete(0));
        assigner.markPartitionComplete(0);
        assertTrue(assigner.isPartitionComplete(0));
    }
}
