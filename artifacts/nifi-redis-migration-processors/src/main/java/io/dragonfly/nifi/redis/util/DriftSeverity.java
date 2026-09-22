package io.dragonfly.nifi.redis.util;

/** How badly a {@link TopologyDiff} compromises an open keyspace subscription. */
public enum DriftSeverity {
    NONE, ADVISORY, CRITICAL
}
