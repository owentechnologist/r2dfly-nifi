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
    private static final String CLAIM_0 = "redis.migration.partition.v2.migration-1.claim.0";

    @Test
    void aClaimStillBeingHeartbeatIsNotTakenOver() throws Exception {
        FakeAtomicMapCacheClient cache = new FakeAtomicMapCacheClient();
        PartitionAssigner owner = new PartitionAssigner(cache, "migration-1");
        PartitionAssigner.Claim claim = owner.claimPartition(1, 60_000).orElseThrow();
        assertEquals(0, claim.partition());
        assertTrue(owner.heartbeat(claim));

        assertEquals(Optional.empty(), new PartitionAssigner(cache, "migration-1").claimPartition(1, 60_000));
    }

    @Test
    void aStaleClaimIsTakenOverByExactlyOneTask() throws Exception {
        FakeAtomicMapCacheClient cache = new FakeAtomicMapCacheClient();
        cache.put(CLAIM_0, String.valueOf(System.currentTimeMillis() - 120_000), STRING, STRING);

        Optional<Integer> first = new PartitionAssigner(cache, "migration-1").claimPartition(1, 60_000).map(PartitionAssigner.Claim::partition);
        Optional<PartitionAssigner.Claim> second = new PartitionAssigner(cache, "migration-1").claimPartition(1, 60_000);

        assertEquals(Optional.of(0), first, "the dead owner's claim is taken over");
        assertEquals(Optional.empty(), second, "the take-over refreshed it, so the next task leaves it alone");
    }

    @Test
    void aFinishedPartitionIsPassedOverForOneADeadNodeLeftUnfinished() throws Exception {
        FakeAtomicMapCacheClient cache = new FakeAtomicMapCacheClient();
        String stale = String.valueOf(System.currentTimeMillis() - 120_000);
        cache.put(CLAIM_0, stale, STRING, STRING);
        cache.put(CLAIM_0.replace(".claim.0", ".claim.1"), stale, STRING, STRING);
        new PartitionAssigner(cache, "migration-1").markPartitionComplete(0);

        assertEquals(Optional.of(1), new PartitionAssigner(cache, "migration-1").claimPartition(2, 60_000).map(PartitionAssigner.Claim::partition));
    }

    @Test
    void aClaimFromBeforeHeartbeatsCountsAsStale() throws Exception {
        FakeAtomicMapCacheClient cache = new FakeAtomicMapCacheClient();
        cache.put(CLAIM_0, "claimed", STRING, STRING);

        assertEquals(Optional.of(0), new PartitionAssigner(cache, "migration-1").claimPartition(1, 60_000).map(PartitionAssigner.Claim::partition));
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

        Optional<Integer> firstClaim = first.claimPartition(2).map(PartitionAssigner.Claim::partition);
        Optional<Integer> secondClaim = second.claimPartition(2).map(PartitionAssigner.Claim::partition);

        assertTrue(firstClaim.isPresent());
        assertTrue(secondClaim.isPresent());
        assertFalse(firstClaim.get().equals(secondClaim.get()), "two claimers must not receive the same partition");

        Optional<PartitionAssigner.Claim> thirdClaim = new PartitionAssigner(cache, "migration-1").claimPartition(2);
        assertTrue(thirdClaim.isEmpty(), "no partitions left to claim");
    }

    @Test
    void anOwnerWhoseClaimWasTakenOverLearnsItAndCannotTakeItBack() throws Exception {
        FakeAtomicMapCacheClient cache = new FakeAtomicMapCacheClient();
        PartitionAssigner stalled = new PartitionAssigner(cache, "migration-1");
        PartitionAssigner.Claim old = stalled.claimPartition(1, 50).orElseThrow();
        Thread.sleep(100);
        PartitionAssigner taker = new PartitionAssigner(cache, "migration-1");
        PartitionAssigner.Claim taken = taker.claimPartition(1, 50).orElseThrow();

        assertFalse(stalled.heartbeat(old), "the stalled owner learns its claim is gone");
        assertTrue(taker.heartbeat(taken), "and has not taken it back from the new owner");
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
