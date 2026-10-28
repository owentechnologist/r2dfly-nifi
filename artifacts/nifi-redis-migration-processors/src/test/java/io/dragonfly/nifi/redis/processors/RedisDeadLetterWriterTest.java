package io.dragonfly.nifi.redis.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisDeadLetterWriterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path dir;

    @Test
    void aKeyThatFailsAgainKeepsOneRecordWithItsEarlierReasons() throws Exception {
        TestRunner writer = writer();
        writer.enqueue("first", Map.of("redis.key", "k", "redis.failure.reason", "the target rejected the DUMP payload"));
        writer.enqueue("second", Map.of("redis.key", "k", "redis.failure.reason", "write timed out"));
        writer.run(2);

        List<Path> records = records();
        assertEquals(1, records.size());
        JsonNode record = MAPPER.readTree(records.get(0).toFile());
        assertEquals("write timed out", record.get("reason").asText());
        assertEquals("the target rejected the DUMP payload", record.get("history").get(0).get("reason").asText());
        assertTrue(record.get("history").get(0).hasNonNull("failed_at"));
        assertEquals("second", new String(java.util.Base64.getDecoder().decode(record.get("content_base64").asText())));
    }

    @Test
    void chunksOfOneKeyKeepTheirOwnRecords() throws Exception {
        TestRunner writer = writer();
        writer.enqueue("part 0", Map.of("redis.key", "big", "redis.chunk.index", "0", "redis.chunk.total", "2", "redis.failure.reason", "r"));
        writer.enqueue("part 1", Map.of("redis.key", "big", "redis.chunk.index", "1", "redis.chunk.total", "2", "redis.failure.reason", "r"));
        writer.run(2);

        assertEquals(2, records().size());
        assertTrue(records().stream().allMatch(p -> p.getFileName().toString().matches("key-[0-9a-f]{64}-chunk-[01]-of-2\\.json")));
    }

    @Test
    void historyIsCappedAtTheTwentyNewestReasons() throws Exception {
        TestRunner writer = writer();
        for (int i = 0; i < 25; i++) {
            writer.enqueue("x", Map.of("redis.key", "k", "redis.failure.reason", "failure " + i));
        }
        writer.run(25);

        JsonNode record = MAPPER.readTree(records().get(0).toFile());
        assertEquals("failure 24", record.get("reason").asText());
        assertEquals(20, record.get("history").size());
        assertEquals("failure 23", record.get("history").get(0).get("reason").asText());
        assertEquals("failure 4", record.get("history").get(19).get("reason").asText());
    }

    private TestRunner writer() {
        TestRunner writer = TestRunners.newTestRunner(RedisDeadLetterWriter.class);
        writer.setProperty(RedisDeadLetterWriter.OUTPUT_DIRECTORY, dir.toString());
        return writer;
    }

    /** Every file in the directory, so a leftover temporary file would show up too. */
    private List<Path> records() throws Exception {
        try (Stream<Path> files = Files.list(dir)) {
            return files.sorted().toList();
        }
    }
}
