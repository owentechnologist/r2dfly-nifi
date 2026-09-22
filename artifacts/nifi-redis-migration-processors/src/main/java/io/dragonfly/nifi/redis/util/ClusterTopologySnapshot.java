package io.dragonfly.nifi.redis.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Immutable view of which masters a source Redis Cluster had, and which hash slots each owned,
 * at one instant. {@code RedisKeyspaceEventConsumer} compares two of these to detect topology
 * drift under a live keyspace subscription.
 *
 * <p>Slots stay a flat {@link Set} here because set equality is exactly the question the diff
 * asks - "same slot ownership?". Ranges are a wire detail of {@link #encode()} only; normalizing
 * them into this type would make two equivalent splits of the same slots compare unequal.
 */
public record ClusterTopologySnapshot(long capturedAtMs, Map<String, MasterNode> mastersById) {

    private static final String VERSION = "v1";

    public ClusterTopologySnapshot {
        mastersById = Map.copyOf(mastersById);
    }

    public static ClusterTopologySnapshot of(long capturedAtMs, Collection<MasterNode> masters) {
        Map<String, MasterNode> byId = new LinkedHashMap<>();
        for (MasterNode master : masters) {
            if (byId.put(master.nodeId(), master) != null) {
                throw new IllegalArgumentException("Duplicate master node id: " + master.nodeId());
            }
        }
        return new ClusterTopologySnapshot(capturedAtMs, byId);
    }

    public record MasterNode(String nodeId, String host, int port, Set<Integer> slots) {

        public MasterNode {
            slots = Set.copyOf(slots);
        }
    }

    public String encode() {
        StringBuilder out = new StringBuilder(VERSION).append('|').append(capturedAtMs);
        for (MasterNode master : mastersById.values()) {
            out.append('\n')
                    .append(master.nodeId()).append('|')
                    .append(master.host()).append('|')
                    .append(master.port()).append('|')
                    .append(encodeRanges(master.slots()));
        }
        return out.toString();
    }

    public static ClusterTopologySnapshot decode(String encoded) {
        if (encoded == null) {
            throw new IllegalArgumentException("Encoded topology is null");
        }
        String[] lines = encoded.split("\n", -1);
        String[] header = lines[0].split("\\|", -1);
        if (header.length != 2 || !VERSION.equals(header[0])) {
            throw new IllegalArgumentException("Unsupported topology encoding header: " + lines[0]);
        }
        List<MasterNode> masters = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            String[] fields = lines[i].split("\\|", -1);
            if (fields.length != 4) {
                throw new IllegalArgumentException("Malformed master line: " + lines[i]);
            }
            masters.add(new MasterNode(fields[0], fields[1], parseNumber(fields[2]), decodeRanges(fields[3])));
        }
        return of(parseTimestamp(header[1]), masters);
    }

    private static String encodeRanges(Set<Integer> slots) {
        List<Integer> sorted = slots.stream().sorted().toList();
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < sorted.size()) {
            int low = sorted.get(i);
            int high = low;
            i++;
            while (i < sorted.size() && sorted.get(i) == high + 1) {
                high = sorted.get(i);
                i++;
            }
            if (out.length() > 0) {
                out.append(',');
            }
            out.append(low);
            if (high != low) {
                out.append('-').append(high);
            }
        }
        return out.toString();
    }

    private static Set<Integer> decodeRanges(String ranges) {
        if (ranges.isEmpty()) {
            return Set.of();
        }
        Set<Integer> slots = new LinkedHashSet<>();
        for (String token : ranges.split(",", -1)) {
            int dash = token.indexOf('-');
            if (dash < 0) {
                slots.add(parseNumber(token));
                continue;
            }
            int low = parseNumber(token.substring(0, dash));
            int high = parseNumber(token.substring(dash + 1));
            if (high < low) {
                throw new IllegalArgumentException("Descending slot range: " + token);
            }
            for (int slot = low; slot <= high; slot++) {
                slots.add(slot);
            }
        }
        return slots;
    }

    private static int parseNumber(String text) {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Malformed number '" + text + "' in encoded topology", e);
        }
    }

    private static long parseTimestamp(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Malformed capture timestamp '" + text + "' in encoded topology", e);
        }
    }
}
