package io.dragonfly.nifi.redis.util;

import org.apache.nifi.distributed.cache.client.Serializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionAssignerTest {

    private static final Serializer<String> STRING = (value, out) -> out.write(value.getBytes(StandardCharsets.UTF_8));
    private static final String CLAIM_0 = "redis.migration.partition.migration-1.claim.0";

    @Test
    void aClaimStillBeingHeartbeatIsNotTakenOver() throws Exception {
        FakeAtomicMapCacheClient cache = new FakeAtomicMapCacheClient();
        PartitionAssigner owner = new PartitionAssigner(cache, "migration-1");
        assertEquals(Optional.of(0), owner.claimPartition(1, 60_000));
        owner.heartbeat(0);

        assertEquals(Optional.empty(), new PartitionAssigner(cache, "migration-1").claimPartition(1, 60_000));
    }

    @Test
    void aStaleClaimIsTakenOverByExactlyOneTask() throws Exception {
        FakeAtomicMapCacheClient cache = new FakeAtomicMapCacheClient();
        cache.put(CLAIM_0, String.valueOf(System.currentTimeMillis() - 120_000), STRING, STRING);

        Optional<Integer> first = new PartitionAssigner(cache, "migration-1").claimPartition(1, 60_000);
        Optional<Integer> second = new PartitionAssigner(cache, "migration-1").claimPartition(1, 60_000);

        assertEquals(Optional.of(0), first, "the dead owner's claim is taken over");
        assertEquals(Optional.empty(), second, "the take-over refreshed it, so the next task leaves it alone");
    }

    @Test
    void aClaimFromBeforeHeartbeatsCountsAsStale() throws Exception {
        FakeAtomicMapCacheClient cache = new FakeAtomicMapCacheClient();
        cache.put(CLAIM_0, "claimed", STRING, STRING);

        assertEquals(Optional.of(0), new PartitionAssigner(cache, "migration-1").claimPartition(1, 60_000));
    }

    @Test
    void aCacheWithoutCompareAndReplaceNeverTakesAClaimOver() throws Exception {
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        cache.put(CLAIM_0, "1", STRING, STRING);

        assertEquals(Optional.empty(), new PartitionAssigner(cache, "migration-1").claimPartition(1, 60_000));
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
