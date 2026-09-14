package io.dragonfly.nifi.redis.util;

/**
 * Thrown by {@link SearchIndexDefinition#toFtCreateArgs()} when a field (currently only VECTOR
 * fields) can't be reconstructed from what {@code FT.INFO} reported - e.g. an algorithm other
 * than FLAT/HNSW, or a missing data_type/dim/distance_metric. Signals "skip this index entirely,
 * not just this field" (silently dropping just the offending field would leave the rebuilt index
 * missing behavior its documents were originally indexed for, which is worse than not migrating
 * it at all) - callers should catch this per-index and continue with the rest.
 */
public class UnsupportedSearchIndexException extends RuntimeException {
    public UnsupportedSearchIndexException(String message) {
        super(message);
    }
}
