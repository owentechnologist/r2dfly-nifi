package io.dragonfly.nifi.redis.util;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChunkReassemblerTest {

    @Test
    void mergesHashFieldsAndTheirTtls() {
        KeyRecord first = new KeyRecord("h", "hash", 1000, null, Map.of("a", "1"));
        first.fieldExpireAtMs = Map.of("a", 5L);
        KeyRecord second = new KeyRecord("h", "hash", 1000, null, Map.of("b", "2"));

        KeyRecord merged = ChunkReassembler.merge(List.of(first, second));

        assertEquals(Map.of("a", "1", "b", "2"), merged.value);
        assertEquals(Map.of("a", 5L), merged.fieldExpireAtMs);
        assertEquals(1000, merged.ttlMs);
    }

    @Test
    void mergesListChunksInOrder() {
        KeyRecord merged = ChunkReassembler.merge(List.of(
                new KeyRecord("l", "list", -1, null, List.of("a", "b")), new KeyRecord("l", "list", -1, null, List.of("c"))));

        assertEquals(List.of("a", "b", "c"), merged.value);
    }
}
