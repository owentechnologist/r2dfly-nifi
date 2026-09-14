package io.dragonfly.nifi.redis.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Re-assembles the multiple FlowFiles emitted for one oversized hash/list key (each carrying
 * {@code redis.chunk.index}/{@code redis.chunk.total} attributes, per spec section 4.2) back
 * into a single {@link KeyRecord} before {@code RedisBatchWriter} issues one write for the key.
 *
 * <p>Only plain {@link KeyRecord} fragments are buffered here - never {@code FlowFile} objects.
 * A NiFi {@code FlowFile} is a snapshot scoped to the {@code ProcessSession} that fetched it;
 * holding one across separate {@code onTrigger} invocations (as chunk arrival naturally spans)
 * would reference a session that may already be closed. The caller removes each incoming chunk
 * FlowFile immediately after handing its record to {@link #accumulate}, and creates one brand
 * new FlowFile from the merged record once the group completes.
 */
public final class ChunkReassembler {

    private static final class PendingGroup {
        final TreeMap<Integer, KeyRecord> recordsByIndex = new TreeMap<>();
        int total;
    }

    private final Map<String, PendingGroup> pendingByGroupKey = new HashMap<>();

    /** Returns the merged record once every chunk has arrived; empty while chunks remain outstanding. */
    public synchronized Optional<KeyRecord> accumulate(String groupKey, KeyRecord record, int chunkIndex, int chunkTotal) {
        PendingGroup group = pendingByGroupKey.computeIfAbsent(groupKey, k -> new PendingGroup());
        group.total = chunkTotal;
        group.recordsByIndex.put(chunkIndex, record);

        if (group.recordsByIndex.size() < group.total) {
            return Optional.empty();
        }
        pendingByGroupKey.remove(groupKey);
        return Optional.of(merge(group.recordsByIndex));
    }

    @SuppressWarnings("unchecked")
    private static KeyRecord merge(TreeMap<Integer, KeyRecord> recordsByIndex) {
        KeyRecord first = recordsByIndex.firstEntry().getValue();
        Object mergedValue = switch (first.type) {
            case "hash" -> {
                Map<String, Object> merged = new LinkedHashMap<>();
                for (KeyRecord chunk : recordsByIndex.values()) {
                    merged.putAll((Map<String, Object>) chunk.value);
                }
                yield merged;
            }
            case "list" -> {
                List<Object> merged = new ArrayList<>();
                for (KeyRecord chunk : recordsByIndex.values()) {
                    merged.addAll((List<Object>) chunk.value);
                }
                yield merged;
            }
            default -> throw new IllegalStateException("Chunking is only supported for hash/list, got: " + first.type);
        };
        return new KeyRecord(first.key, first.type, first.ttlMs, first.encoding, mergedValue);
    }
}
