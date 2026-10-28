package io.dragonfly.nifi.redis.util;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Joins the FlowFiles RedisTypeDeserializer emits for one oversized hash or list key (each with
 * {@code redis.chunk.index} and {@code redis.chunk.total}) back into one key record. The deserializer
 * emits a key's chunks in one session, so they reach a writer's queue together, and a writer takes them
 * in one batch. Nothing is held between triggers: a group still missing chunks goes back to the queue,
 * so a restart or a rolled-back session loses nothing, and after {@link #MAX_WAITS} tries its chunks go
 * to failure with the reason.
 */
public final class ChunkReassembler {

    public static final String INDEX = "redis.chunk.index";
    public static final String TOTAL = "redis.chunk.total";
    static final String WAITS = "redis.chunk.waits";
    static final int MAX_WAITS = 10;

    /** One chunk FlowFile with its parsed record. */
    public record Chunk(FlowFile flowFile, KeyRecord record) {
        int index() {
            return Integer.parseInt(flowFile.getAttribute(INDEX));
        }

        int total() {
            return Integer.parseInt(flowFile.getAttribute(TOTAL));
        }
    }

    private ChunkReassembler() {
    }

    public static boolean isChunk(FlowFile flowFile) {
        return flowFile.getAttribute(INDEX) != null && flowFile.getAttribute(TOTAL) != null;
    }

    /**
     * The merged FlowFile and record when {@code chunks}, all of one key, hold every chunk: the chunk
     * FlowFiles are removed and the merged one carries the whole record as its content, so a retry
     * or a dead-letter record of it has the data. Otherwise the chunks go back to the queue, or to
     * {@code failure} once they have waited too often, and the answer is empty.
     */
    public static Optional<Chunk> join(ProcessSession session, List<Chunk> chunks, Relationship failure) {
        TreeMap<Integer, Chunk> byIndex = new TreeMap<>();
        chunks.forEach(chunk -> byIndex.putIfAbsent(chunk.index(), chunk));
        int total = chunks.get(0).total();
        if (byIndex.size() < total) {
            for (Chunk chunk : chunks) {
                String before = chunk.flowFile().getAttribute(WAITS);
                int waits = 1 + (before == null ? 0 : Integer.parseInt(before));
                FlowFile waited = session.putAttribute(chunk.flowFile(), WAITS, String.valueOf(waits));
                if (waits > MAX_WAITS) {
                    session.transfer(FailureReason.tag(session, waited, "only " + byIndex.size() + " of the key's " + total
                            + " chunks arrived; the others failed earlier in the flow"), failure);
                } else {
                    session.transfer(session.penalize(waited));
                }
            }
            return Optional.empty();
        }
        KeyRecord merged = merge(byIndex.values().stream().map(Chunk::record).toList());
        FlowFile first = byIndex.firstEntry().getValue().flowFile();
        FlowFile mergedFlowFile = session.removeAllAttributes(session.create(first), Set.of(INDEX, TOTAL, WAITS));
        mergedFlowFile = session.write(mergedFlowFile, out -> RedisTypeSerializer.writeJson(merged, out));
        chunks.forEach(chunk -> session.remove(chunk.flowFile()));
        return Optional.of(new Chunk(mergedFlowFile, merged));
    }

    /** Up to {@code size} FlowFiles, plus the remaining chunks of any chunked key already taken. */
    public static FlowFileFilter batchOf(int size) {
        Set<String> chunkedKeys = new HashSet<>();
        int[] taken = {0};
        return flowFile -> {
            byte[] keyBytes = RedisBytes.key(flowFile);
            String key = keyBytes == null ? null : RedisBytes.identity(keyBytes);
            boolean chunk = isChunk(flowFile);
            if (chunk && chunkedKeys.contains(key)) {
                return FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
            }
            if (taken[0] >= size) {
                return chunkedKeys.isEmpty() ? FlowFileFilter.FlowFileFilterResult.REJECT_AND_TERMINATE
                        : FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;
            }
            taken[0]++;
            if (chunk) {
                chunkedKeys.add(key);
            }
            return FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
        };
    }

    /** Groups chunks by key, in the order each key first appears; keyed by {@link RedisBytes#identity}. */
    public static Map<String, List<Chunk>> byKey(List<Chunk> chunks) {
        Map<String, List<Chunk>> groups = new LinkedHashMap<>();
        chunks.forEach(chunk -> groups.computeIfAbsent(RedisBytes.identity(chunk.record().keyBytes()), k -> new ArrayList<>()).add(chunk));
        return groups;
    }

    @SuppressWarnings("unchecked")
    static KeyRecord merge(List<KeyRecord> inOrder) {
        KeyRecord first = inOrder.get(0);
        Object mergedValue = switch (first.type) {
            case "hash" -> {
                Map<String, Object> merged = new LinkedHashMap<>();
                inOrder.forEach(chunk -> merged.putAll((Map<String, Object>) chunk.value));
                yield merged;
            }
            case "list" -> {
                List<Object> merged = new ArrayList<>();
                inOrder.forEach(chunk -> merged.addAll((List<Object>) chunk.value));
                yield merged;
            }
            default -> throw new IllegalStateException("Chunking is only supported for hash/list, got: " + first.type);
        };
        KeyRecord merged = new KeyRecord(first.key, first.type, first.ttlMs, first.encoding, mergedValue);
        merged.keyBase64 = first.keyBase64;
        merged.base64 = first.base64;
        for (KeyRecord chunk : inOrder) {
            if (chunk.fieldExpireAtMs != null) {
                if (merged.fieldExpireAtMs == null) {
                    merged.fieldExpireAtMs = new LinkedHashMap<>();
                }
                merged.fieldExpireAtMs.putAll(chunk.fieldExpireAtMs);
            }
        }
        return merged;
    }
}
