package io.dragonfly.nifi.redis.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dragonfly.nifi.redis.processors.RedisBatchWriter;
import io.dragonfly.nifi.redis.processors.RedisDeadLetterWriter;
import io.dragonfly.nifi.redis.processors.RedisTypeDeserializer;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A key the target refuses reaches the dead-letter directory with the reason it failed, and the
 * pending count follows the records an operator moves away. Requires Docker; run via {@code mvn verify}.
 */
@Testcontainers
class DeadLetterIT {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4")).withExposedPorts(6379);

    @Container
    static GenericContainer<?> dragonfly = new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest"))
            .withExposedPorts(6379);

    @Test
    void aRefusedWriteIsDeadLetteredWithItsReason(@TempDir Path deadLetters) throws Exception {
        try (Client src = new Client(redis); Client tgt = new Client(dragonfly)) {
            src.connection.sync().flushall();
            tgt.connection.sync().flushall();
            src.connection.sync().set("taken", "from the source");
            src.connection.sync().set("free", "from the source");
            tgt.connection.sync().set("taken", "already on the target");
        }

        TestRunner deserializer = TestRunners.newTestRunner(RedisTypeDeserializer.class);
        deserializer.setAllowSynchronousSessionCommits(true);
        addPool(deserializer, "source-pool", new StandardRedisConnectionPoolService(), redis);
        deserializer.setProperty(RedisTypeDeserializer.REDIS_CONNECTION_POOL, "source-pool");
        deserializer.enqueue(new byte[0], Map.of("redis.key", "taken", "redis.type", "string"));
        deserializer.enqueue(new byte[0], Map.of("redis.key", "free", "redis.type", "string"));
        deserializer.run();

        TestRunner writer = TestRunners.newTestRunner(RedisBatchWriter.class);
        writer.setAllowSynchronousSessionCommits(true);
        addPool(writer, "target-pool", new StandardDragonflyConnectionPoolService(), dragonfly);
        writer.setProperty(RedisBatchWriter.DRAGONFLY_CONNECTION_POOL, "target-pool");
        writer.setProperty(RedisBatchWriter.CONFLICT_STRATEGY, "FAIL");
        forward(deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_SUCCESS), writer);
        writer.run();
        assertEquals(1L, writer.getCounterValue("Keys Written"));

        TestRunner dlw = deadLetterWriter(deadLetters);
        forward(writer.getFlowFilesForRelationship(RedisBatchWriter.REL_FAILURE), dlw);
        dlw.run();
        dlw.assertAllFlowFilesTransferred(RedisDeadLetterWriter.REL_SUCCESS, 1);
        assertEquals(1L, dlw.getCounterValue("Dead-Letter Records Pending"), "counted as it is written, not at the next recount");

        List<Path> records;
        try (Stream<Path> files = Files.list(deadLetters)) {
            records = files.toList();
        }
        assertEquals(1, records.size());
        JsonNode record = new ObjectMapper().readTree(records.get(0).toFile());
        assertEquals("taken", record.get("attributes").get("redis.key").asText());
        assertEquals("the key already exists on the target and conflict-strategy is FAIL", record.get("reason").asText());

        TestRunner later = deadLetterWriter(deadLetters);
        later.run();
        assertEquals(1L, later.getCounterValue("Dead-Letter Records Pending"));
        Path reviewed = Files.createDirectory(deadLetters.resolve("reviewed"));
        Files.move(records.get(0), reviewed.resolve(records.get(0).getFileName()));
        TestRunner afterReview = deadLetterWriter(deadLetters);
        afterReview.run();
        assertEquals(null, afterReview.getCounterValue("Dead-Letter Records Pending"), "nothing pending once it is moved away");
    }

    private static TestRunner deadLetterWriter(Path directory) {
        TestRunner runner = TestRunners.newTestRunner(RedisDeadLetterWriter.class);
        runner.setProperty(RedisDeadLetterWriter.OUTPUT_DIRECTORY, directory.toString());
        return runner;
    }

    private static void forward(List<MockFlowFile> flowFiles, TestRunner to) {
        flowFiles.forEach(f -> to.enqueue(f.toByteArray(), f.getAttributes()));
    }

    private static void addPool(TestRunner runner, String id, ControllerService pool, GenericContainer<?> container) throws Exception {
        runner.addControllerService(id, pool);
        runner.setProperty(pool, AbstractRedisConnectionPoolService.CONNECTION_STRING, url(container));
        runner.setProperty(pool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        runner.enableControllerService(pool);
    }

    private static String url(GenericContainer<?> container) {
        return "redis://" + container.getHost() + ":" + container.getMappedPort(6379);
    }

    private static final class Client implements AutoCloseable {
        private final RedisClient client;
        private final StatefulRedisConnection<String, String> connection;

        Client(GenericContainer<?> container) {
            client = RedisClient.create(url(container));
            connection = client.connect();
        }

        @Override
        public void close() {
            connection.close();
            client.shutdown();
        }
    }
}
