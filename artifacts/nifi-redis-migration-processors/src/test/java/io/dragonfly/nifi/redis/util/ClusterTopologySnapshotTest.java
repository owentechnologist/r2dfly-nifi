package io.dragonfly.nifi.redis.util;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterTopologySnapshotTest {

    @Test
    void encodeDecodeRoundTripsAMultiRangeSlotSet() {
        ClusterTopologySnapshot snapshot = ClusterTopologySnapshot.of(1700000000000L, List.of(
                new ClusterTopologySnapshot.MasterNode("node-a", "10.0.0.1", 6379, Set.of(0, 1, 2, 7, 100, 101, 102)),
                new ClusterTopologySnapshot.MasterNode("node-b", "10.0.0.2", 6380, Set.of(16383))));

        String encoded = snapshot.encode();

        assertTrue(encoded.contains("node-a|10.0.0.1|6379|0-2,7,100-102"), encoded);
        assertTrue(encoded.contains("node-b|10.0.0.2|6380|16383"), encoded);
        assertEquals(snapshot, ClusterTopologySnapshot.decode(encoded));
    }

    @Test
    void anEmptyMasterSetRoundTrips() {
        ClusterTopologySnapshot snapshot = ClusterTopologySnapshot.of(42L, List.of());

        assertEquals("v1|42", snapshot.encode());
        assertEquals(snapshot, ClusterTopologySnapshot.decode(snapshot.encode()));
    }

    @Test
    void aMasterWithNoSlotsRoundTrips() {
        ClusterTopologySnapshot snapshot = ClusterTopologySnapshot.of(42L, List.of(
                new ClusterTopologySnapshot.MasterNode("node-a", "10.0.0.1", 6379, Set.of())));

        assertEquals("v1|42\nnode-a|10.0.0.1|6379|", snapshot.encode());
        assertEquals(snapshot, ClusterTopologySnapshot.decode(snapshot.encode()));
    }

    @Test
    void decodeRejectsAnUnknownVersion() {
        assertThrows(IllegalArgumentException.class,
                () -> ClusterTopologySnapshot.decode("v2|42\nnode-a|10.0.0.1|6379|0-2"));
    }

    @Test
    void decodeRejectsMalformedInput() {
        assertThrows(IllegalArgumentException.class, () -> ClusterTopologySnapshot.decode(""));
        assertThrows(IllegalArgumentException.class, () -> ClusterTopologySnapshot.decode("v1|not-a-timestamp"));
        assertThrows(IllegalArgumentException.class, () -> ClusterTopologySnapshot.decode("v1|42\nnode-a|10.0.0.1|6379"));
        assertThrows(IllegalArgumentException.class, () -> ClusterTopologySnapshot.decode("v1|42\nnode-a|10.0.0.1|6379|5-1"));
        assertThrows(IllegalArgumentException.class, () -> ClusterTopologySnapshot.decode("v1|42\nnode-a|10.0.0.1|6379|zero"));
    }
}
