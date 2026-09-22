package io.dragonfly.nifi.redis.util;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopologyDiffTest {

    @Test
    void newlyJoinedMasterIsCritical() {
        ClusterTopologySnapshot baseline = ClusterTopologySnapshot.of(1L, List.of(master("a", 0, 8191)));
        ClusterTopologySnapshot current = ClusterTopologySnapshot.of(2L, List.of(master("a", 0, 8191), master("b", 8192, 16383)));

        TopologyDiff diff = TopologyDiff.between(baseline, current);

        assertEquals(DriftSeverity.CRITICAL, diff.severity());
        assertEquals(Set.of("b"), diff.newlyJoinedMasterIds());
        assertEquals(Set.of(), diff.departedMasterIds());
        assertEquals(Set.of(), diff.slotReassignedMasterIds());
    }

    @Test
    void departedMasterAloneIsAdvisory() {
        ClusterTopologySnapshot baseline = ClusterTopologySnapshot.of(1L, List.of(master("a", 0, 8191), master("b", 8192, 16383)));
        ClusterTopologySnapshot current = ClusterTopologySnapshot.of(2L, List.of(master("a", 0, 8191)));

        TopologyDiff diff = TopologyDiff.between(baseline, current);

        assertEquals(DriftSeverity.ADVISORY, diff.severity());
        assertEquals(Set.of("b"), diff.departedMasterIds());
        assertEquals(Set.of(), diff.newlyJoinedMasterIds());
        assertEquals(Set.of(), diff.slotReassignedMasterIds());
    }

    @Test
    void slotReassignmentAloneIsAdvisory() {
        ClusterTopologySnapshot baseline = ClusterTopologySnapshot.of(1L, List.of(master("a", 0, 8191), master("b", 8192, 16383)));
        ClusterTopologySnapshot current = ClusterTopologySnapshot.of(2L, List.of(master("a", 0, 4095), master("b", 4096, 16383)));

        TopologyDiff diff = TopologyDiff.between(baseline, current);

        assertEquals(DriftSeverity.ADVISORY, diff.severity());
        assertEquals(Set.of("a", "b"), diff.slotReassignedMasterIds());
        assertEquals(Set.of(), diff.newlyJoinedMasterIds());
        assertEquals(Set.of(), diff.departedMasterIds());
    }

    @Test
    void identicalSnapshotsHaveNoDrift() {
        ClusterTopologySnapshot baseline = ClusterTopologySnapshot.of(1L, List.of(master("a", 0, 8191), master("b", 8192, 16383)));
        ClusterTopologySnapshot current = ClusterTopologySnapshot.of(2L, List.of(master("a", 0, 8191), master("b", 8192, 16383)));

        TopologyDiff diff = TopologyDiff.between(baseline, current);

        assertFalse(diff.hasDrift());
        assertEquals(DriftSeverity.NONE, diff.severity());
    }

    @Test
    void aNewMasterAlongsideADepartureIsStillCritical() {
        ClusterTopologySnapshot baseline = ClusterTopologySnapshot.of(1L, List.of(master("a", 0, 8191), master("b", 8192, 16383)));
        ClusterTopologySnapshot current = ClusterTopologySnapshot.of(2L, List.of(master("a", 0, 8191), master("c", 8192, 16383)));

        TopologyDiff diff = TopologyDiff.between(baseline, current);

        assertTrue(diff.hasDrift());
        assertEquals(DriftSeverity.CRITICAL, diff.severity());
        assertEquals(Set.of("c"), diff.newlyJoinedMasterIds());
        assertEquals(Set.of("b"), diff.departedMasterIds());
        assertEquals(Set.of(), diff.slotReassignedMasterIds());
    }

    private static ClusterTopologySnapshot.MasterNode master(String nodeId, int firstSlot, int lastSlot) {
        Set<Integer> slots = IntStream.rangeClosed(firstSlot, lastSlot).boxed().collect(Collectors.toSet());
        return new ClusterTopologySnapshot.MasterNode(nodeId, "host-" + nodeId, 6379, slots);
    }
}
