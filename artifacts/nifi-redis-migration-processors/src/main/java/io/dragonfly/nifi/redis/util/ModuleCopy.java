package io.dragonfly.nifi.redis.util;

import java.util.Map;

/**
 * How a key of a module type gets to Dragonfly, by the TYPE the source reports. Measured against
 * Dragonfly 2.0.2 with Redis Stack 7.4 and Redis 8.10 sources: RESTORE accepts only ReJSON-RL
 * payloads (and none at all from Redis 8, whose DUMP version is newer), BF.LOADCHUNK rejects
 * RedisBloom's BF.SCANDUMP chunks, and there is no CF.SCANDUMP/LOADCHUNK, TS.*, TDIGEST.*, GRAPH.*
 * or VADD. So JSON and TopK are rebuilt from their values, Bloom, Cuckoo and Count-Min Sketch can
 * only be copied by RESTORE (which works between Dragonfly instances), and the rest have nowhere
 * to go.
 */
public enum ModuleCopy {
    JSON_REBUILD,
    TOPK_REBUILD,
    /** RESTORE, and dead-letter when the target rejects the payload. Also every unknown module type. */
    RESTORE_ONLY,
    /** Dead-lettered without an attempt. */
    NOT_SUPPORTED_BY_DRAGONFLY;

    private static final Map<String, ModuleCopy> BY_TYPE = Map.of(
            "rejson-rl", JSON_REBUILD,
            "topk-type", TOPK_REBUILD,
            "mbbloom--", RESTORE_ONLY,
            "mbbloomcf", RESTORE_ONLY,
            "cmsk-type", RESTORE_ONLY,
            "tsdb-type", NOT_SUPPORTED_BY_DRAGONFLY,
            "tdis-type", NOT_SUPPORTED_BY_DRAGONFLY,
            "graphdata", NOT_SUPPORTED_BY_DRAGONFLY,
            "vectorset", NOT_SUPPORTED_BY_DRAGONFLY);

    public static ModuleCopy of(String type) {
        return BY_TYPE.getOrDefault(type.toLowerCase(), RESTORE_ONLY);
    }

    /** Why a key of {@code type} was not copied, given the target's RESTORE error when one was tried. */
    public static String notCopiedReason(String type, String restoreError) {
        return switch (of(type)) {
            case NOT_SUPPORTED_BY_DRAGONFLY -> type + " is not supported by Dragonfly";
            case RESTORE_ONLY -> (BY_TYPE.containsKey(type.toLowerCase()) ? "no lossless copy path for " + type + " on this Dragonfly version"
                    : "unknown module type " + type) + "; RESTORE was rejected: " + restoreError;
            default -> type + " could not be rebuilt: " + restoreError;
        };
    }
}
