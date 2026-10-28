package io.dragonfly.nifi.redis.processors;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyRecordFileTest {

    @TempDir
    Path dir;

    @Test
    void writesTheCommandsATargetWouldReceive() throws Exception {
        Path file = dir.resolve("out.aof");
        TestRunner writer = writer(file, "resp");
        writer.enqueue("{\"key\":\"k\",\"type\":\"string\",\"ttl_ms\":5000,\"value\":\"v\"}");
        writer.enqueue("{\"key\":\"gone\",\"type\":\"deleted\",\"ttl_ms\":-1}");
        writer.enqueue("{\"key\":\"l\",\"type\":\"list\",\"ttl_ms\":0,\"value\":[\"a\",\"b\"]}");
        writer.run();

        assertEquals("*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$4\r\n5000\r\n"
                        + "*2\r\n$3\r\nDEL\r\n$4\r\ngone\r\n"
                        + "*2\r\n$3\r\nDEL\r\n$1\r\nl\r\n*4\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\na\r\n$1\r\nb\r\n",
                Files.readString(file));
    }

    @Test
    void joinsAKeysChunksEvenPastTheBatchSizeAndAppendsAcrossRuns() throws Exception {
        Path file = dir.resolve("out.jsonl");
        TestRunner writer = writer(file, "jsonl");
        writer.setProperty(KeyRecordFileWriter.BATCH_SIZE, "1");
        writer.enqueue("{\"key\":\"h\",\"type\":\"hash\",\"ttl_ms\":0,\"value\":{\"a\":\"1\"}}", chunk("h", 0, 3));
        writer.enqueue("{\"key\":\"h\",\"type\":\"hash\",\"ttl_ms\":0,\"value\":{\"b\":\"2\"}}", chunk("h", 1, 3));
        writer.enqueue("{\"key\":\"h\",\"type\":\"hash\",\"ttl_ms\":0,\"value\":{\"c\":\"3\"}}", chunk("h", 2, 3));
        writer.enqueue("{\"key\":\"s\",\"type\":\"string\",\"ttl_ms\":0,\"value\":\"x\"}");
        writer.run();
        assertEquals(1, writer.getQueueSize().getObjectCount(), "the three chunks were one batch, the string waits");
        writer.run();

        assertEquals(List.of("{\"key\":\"h\",\"type\":\"hash\",\"value\":{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"},\"ttl_ms\":0}",
                "{\"key\":\"s\",\"type\":\"string\",\"value\":\"x\",\"ttl_ms\":0}"), Files.readAllLines(file));
    }

    @Test
    void aKeyMissingAChunkWaitsInTheQueueThenFailsWithAReason() throws Exception {
        Path file = dir.resolve("out.jsonl");
        TestRunner writer = writer(file, "jsonl");
        writer.enqueue("{\"key\":\"h\",\"type\":\"hash\",\"ttl_ms\":0,\"value\":{\"a\":\"1\"}}", chunk("h", 0, 2));

        writer.run();
        assertEquals(1, writer.getQueueSize().getObjectCount(), "an incomplete key goes back to the queue, not into memory");
        assertEquals("", Files.readString(file));
        writer.run(10);

        MockFlowFile failed = writer.getFlowFilesForRelationship(KeyRecordFileWriter.REL_FAILURE).get(0);
        assertEquals("only 1 of the key's 2 chunks arrived; the others failed earlier in the flow", failed.getAttribute("redis.failure.reason"));
    }

    private static Map<String, String> chunk(String key, int index, int total) {
        return Map.of("redis.key", key, "redis.chunk.index", String.valueOf(index), "redis.chunk.total", String.valueOf(total));
    }

    @Test
    void sendsADumpPayloadToFailureWithAReason() {
        TestRunner writer = writer(dir.resolve("out.jsonl"), "jsonl");
        writer.enqueue(new byte[] {1, 2}, Map.of("redis.dfly-dump", "true", "redis.key", "k"));
        writer.run();

        MockFlowFile failed = writer.getFlowFilesForRelationship(KeyRecordFileWriter.REL_FAILURE).get(0);
        assertTrue(failed.getAttribute("redis.failure.reason").startsWith("a DUMP payload cannot be written to a file"));
    }

    @Test
    void readsInBatchesResumesAndReportsBadLines() throws Exception {
        Path file = dir.resolve("in.jsonl");
        Files.writeString(file, "{\"key\":\"a\",\"type\":\"string\",\"value\":\"1\"}\n"
                + "not json\n"
                + "\n"
                + "{\"key\":\"b\",\"type\":\"string\",\"value\":\"2\"}\n"
                + "{\"key\":\"c\",\"type\":\"deleted\"}", StandardCharsets.UTF_8);
        TestRunner reader = TestRunners.newTestRunner(KeyRecordFileReader.class);
        reader.setProperty(KeyRecordFileReader.FILE, file.toString());
        reader.setProperty(KeyRecordFileReader.RECORDS_PER_TRIGGER, "2");

        reader.run();
        assertNotEquals(1L, reader.getCounterValue("File Read Complete"));
        assertEquals(List.of("a"), keys(reader.getFlowFilesForRelationship(KeyRecordFileReader.REL_SUCCESS)));
        MockFlowFile bad = reader.getFlowFilesForRelationship(KeyRecordFileReader.REL_FAILURE).get(0);
        assertTrue(bad.getAttribute("redis.failure.reason").startsWith("the line at byte 40 is not a key record"));
        bad.assertContentEquals("not json");

        reader.run(2);
        assertEquals(List.of("a", "b", "c"), keys(reader.getFlowFilesForRelationship(KeyRecordFileReader.REL_SUCCESS)));
        assertEquals(1, reader.getCounterValue("File Read Complete"));
        assertEquals(Long.toString(Files.size(file)), reader.getStateManager().getState(Scope.LOCAL).get("offset"));

        reader.clearTransferState();
        reader.run();
        assertEquals(List.of(), reader.getFlowFilesForRelationship(KeyRecordFileReader.REL_SUCCESS), "a finished file is not read again");
    }

    private static TestRunner writer(Path file, String format) {
        TestRunner writer = TestRunners.newTestRunner(KeyRecordFileWriter.class);
        writer.setProperty(KeyRecordFileWriter.FILE, file.toString());
        writer.setProperty(KeyRecordFileWriter.FORMAT, format);
        return writer;
    }

    private static List<String> keys(List<MockFlowFile> flowFiles) {
        return flowFiles.stream().map(f -> f.getAttribute("redis.key")).toList();
    }
}
