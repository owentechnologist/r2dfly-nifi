package io.dragonfly.nifi.redis.util;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * What changed between two {@link ClusterTopologySnapshot}s of the same source cluster, and how
 * much it matters to an already-open keyspace subscription.
 */
public record TopologyDiff(Set<String> newlyJoinedMasterIds,
                           Set<String> departedMasterIds,
                           Set<String> slotReassignedMasterIds) {

    public TopologyDiff {
        newlyJoinedMasterIds = Set.copyOf(newlyJoinedMasterIds);
        departedMasterIds = Set.copyOf(departedMasterIds);
        slotReassignedMasterIds = Set.copyOf(slotReassignedMasterIds);
    }

    public static TopologyDiff between(ClusterTopologySnapshot baseline, ClusterTopologySnapshot current) {
        Set<String> newlyJoined = new LinkedHashSet<>(current.mastersById().keySet());
        newlyJoined.removeAll(baseline.mastersById().keySet());

        Set<String> departed = new LinkedHashSet<>(baseline.mastersById().keySet());
        departed.removeAll(current.mastersById().keySet());

        Set<String> slotReassigned = new LinkedHashSet<>();
        for (Map.Entry<String, ClusterTopologySnapshot.MasterNode> entry : baseline.mastersById().entrySet()) {
            ClusterTopologySnapshot.MasterNode currentNode = current.mastersById().get(entry.getKey());
            // Address is deliberately not compared: a master reported under a different host/port
            // but holding the same id and slots is the same subscription target.
            if (currentNode != null && !currentNode.slots().equals(entry.getValue().slots())) {
                slotReassigned.add(entry.getKey());
            }
        }
        return new TopologyDiff(newlyJoined, departed, slotReassigned);
    }

    public boolean hasDrift() {
        return severity() != DriftSeverity.NONE;
    }

    /**
     * Only a newly joined master is CRITICAL: a subscription opened before that master existed
     * never subscribed to it, so every event on its slots has been silently dropped since it
     * joined. A departure or a slot reassignment is ADVISORY because nothing was missed - those
     * slots moved to (or failed over to) a node already being subscribed to.
     */
    public DriftSeverity severity() {
        if (!newlyJoinedMasterIds.isEmpty()) {
            return DriftSeverity.CRITICAL;
        }
        if (!departedMasterIds.isEmpty() || !slotReassignedMasterIds.isEmpty()) {
            return DriftSeverity.ADVISORY;
        }
        return DriftSeverity.NONE;
    }
}
