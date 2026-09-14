package io.dragonfly.nifi.redis.util;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkReassemblerTest {

    @Test
    void staysEmptyUntilAllChunksArriveThenMergesHashFields() {
        ChunkReassembler reassembler = new ChunkReassembler();

        Map<String, Object> chunk0Fields = new LinkedHashMap<>();
        chunk0Fields.put("a", "1");
        KeyRecord chunk0 = new KeyRecord("myhash", "hash", 1000, null, chunk0Fields);

        Map<String, Object> chunk1Fields = new LinkedHashMap<>();
        chunk1Fields.put("b", "2");
        KeyRecord chunk1 = new KeyRecord("myhash", "hash", 1000, null, chunk1Fields);

        Optional<KeyRecord> afterFirst = reassembler.accumulate("myhash", chunk0, 0, 2);
        assertTrue(afterFirst.isEmpty(), "group must not complete until every chunk arrives");

        Optional<KeyRecord> afterSecond = reassembler.accumulate("myhash", chunk1, 1, 2);
        assertTrue(afterSecond.isPresent());

        @SuppressWarnings("unchecked")
        Map<String, Object> merged = (Map<String, Object>) afterSecond.get().value;
        assertEquals(Map.of("a", "1", "b", "2"), merged);
        assertEquals("myhash", afterSecond.get().key);
        assertEquals(1000, afterSecond.get().ttlMs);
    }

    @Test
    void mergesListChunksInOrder() {
        ChunkReassembler reassembler = new ChunkReassembler();

        KeyRecord chunk0 = new KeyRecord("mylist", "list", -1, null, List.of("a", "b"));
        KeyRecord chunk1 = new KeyRecord("mylist", "list", -1, null, List.of("c", "d"));

        reassembler.accumulate("mylist", chunk0, 0, 2);
        Optional<KeyRecord> completed = reassembler.accumulate("mylist", chunk1, 1, 2);

        assertTrue(completed.isPresent());
        assertEquals(List.of("a", "b", "c", "d"), completed.get().value);
    }

    @Test
    void independentGroupsDoNotInterfere() {
        ChunkReassembler reassembler = new ChunkReassembler();
        KeyRecord keyAChunk0 = new KeyRecord("keyA", "list", -1, null, List.of("1"));
        KeyRecord keyBChunk0 = new KeyRecord("keyB", "list", -1, null, List.of("x"));

        assertTrue(reassembler.accumulate("keyA", keyAChunk0, 0, 2).isEmpty());
        assertTrue(reassembler.accumulate("keyB", keyBChunk0, 0, 2).isEmpty());

        Optional<KeyRecord> keyBCompleted = reassembler.accumulate("keyB", new KeyRecord("keyB", "list", -1, null, List.of("y")), 1, 2);
        assertTrue(keyBCompleted.isPresent());
        assertEquals(List.of("x", "y"), keyBCompleted.get().value);
    }
}
