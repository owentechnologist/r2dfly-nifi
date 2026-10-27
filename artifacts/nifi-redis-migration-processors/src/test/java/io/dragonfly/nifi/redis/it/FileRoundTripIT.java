package io.dragonfly.nifi.redis.it;

import io.dragonfly.nifi.redis.processors.KeyRecordFileReader;
import io.dragonfly.nifi.redis.processors.KeyRecordFileWriter;
import io.dragonfly.nifi.redis.processors.RedisBatchWriter;
import io.dragonfly.nifi.redis.processors.RedisScanReader;
import io.dragonfly.nifi.redis.processors.RedisTypeDeserializer;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.FakeDistributedMapCacheClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.dragonfly.nifi.redis.it.ReplicationPipeline.addPool;
import static io.dragonfly.nifi.redis.it.ReplicationPipeline.assertTargetMatchesSource;
import static io.dragonfly.nifi.redis.it.ReplicationPipeline.url;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Redis to a JSON-lines file to Dragonfly, and Redis to a RESP file loaded by a fresh Redis as
 * its AOF, both ending identical to the source. Requires Docker; run via {@code mvn verify}.
 */
class FileRoundTripIT {

    private static final GenericContainer<?> SOURCE = new GenericContainer<>(DockerImageName.parse("redis:7.4")).withExposedPorts(6379);
    private static final GenericContainer<?> DRAGONFLY =
            new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest")).withExposedPorts(6379);

    @TempDir
    Path dir;

    @BeforeAll
    static void seed() throws Exception {
        SOURCE.start();
        DRAGONFLY.start();
        try (RedisClient client = RedisClient.create(url(SOURCE)); StatefulRedisConnection<String, String> c = client.connect()) {
            RedisCommands<String, String> s = c.sync();
            s.set("s", "plain");
            s.set("s:ttl", "expiring", SetArgs.Builder.ex(3600));
            s.set("s:utf8", "zürich ✓\nsecond line");
            Map<String, String> big = new LinkedHashMap<>();
            IntStream.range(0, 25).forEach(i -> big.put("f" + i, "v" + i));
            s.hset("h:big", big);
            s.rpush("l", "a", "b", "a");
            s.sadd("set", "x", "y");
            s.zadd("z", 1.5, "one", 2.0, "two");
            s.pexpire("z", 600_000);
            for (int i = 1; i <= 4; i++) {
                s.xadd("st", new XAddArgs().id(i + "-0"), Map.of("n", String.valueOf(i)));
            }
            s.xgroupCreate(XReadArgs.StreamOffset.from("st", "0"), "g", XGroupCreateArgs.Builder.mkstream(false));
            s.xreadgroup(Consumer.from("g", "c1"), XReadArgs.Builder.count(2), XReadArgs.StreamOffset.lastConsumed("st"));
        }
    }

    @AfterAll
    static void stop() {
        SOURCE.stop();
        DRAGONFLY.stop();
    }

    @Test
    void jsonLinesFileRebuildsTheSourceOnDragonfly() throws Exception {
        Path file = dir.resolve("backup.jsonl");
        writeFile(file, KeyRecordFileWriter.FORMAT_JSONL.getValue());
        assertEquals(8, Files.readAllLines(file).size(), "one line per key, the chunked hash joined");

        TestRunner reader = TestRunners.newTestRunner(KeyRecordFileReader.class);
        reader.setAllowSynchronousSessionCommits(true);
        reader.setProperty(KeyRecordFileReader.FILE, file.toString());
        reader.setProperty(KeyRecordFileReader.RECORDS_PER_TRIGGER, "3");
        reader.run(4);
        assertEquals(1, reader.getCounterValue("File Read Complete"));

        TestRunner writer = TestRunners.newTestRunner(RedisBatchWriter.class);
        writer.setAllowSynchronousSessionCommits(true);
        addPool(writer, "target-pool", new StandardDragonflyConnectionPoolService(), url(DRAGONFLY), "STANDALONE");
        writer.setProperty(RedisBatchWriter.DRAGONFLY_CONNECTION_POOL, "target-pool");
        reader.getFlowFilesForRelationship(KeyRecordFileReader.REL_SUCCESS).forEach(f -> writer.enqueue(f.toByteArray(), f.getAttributes()));
        writer.run();
        assertEquals(List.of(), writer.getFlowFilesForRelationship(RedisBatchWriter.REL_FAILURE));

        assertSameAsSource(DRAGONFLY);
    }

    @Test
    void respFileLoadsAsAnAof() throws Exception {
        Path file = dir.resolve("backup.aof");
        writeFile(file, KeyRecordFileWriter.FORMAT_RESP.getValue());

        try (GenericContainer<?> loaded = new GenericContainer<>(DockerImageName.parse("redis:7.4"))
                .withExposedPorts(6379)
                .withCopyToContainer(Transferable.of(Files.readAllBytes(file)), "/data/appendonly.aof")
                .withCommand("redis-server", "--appendonly", "yes")
                .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1))) {
            loaded.start();
            assertSameAsSource(loaded);
        }
    }

    private static void writeFile(Path file, String format) throws Exception {
        TestRunner scan = TestRunners.newTestRunner(RedisScanReader.class);
        scan.setAllowSynchronousSessionCommits(true);
        addPool(scan, "source-pool", new StandardRedisConnectionPoolService(), url(SOURCE), "STANDALONE");
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        scan.addControllerService("cursor-cache", cache);
        scan.enableControllerService(cache);
        scan.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "source-pool");
        scan.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cursor-cache");
        scan.setProperty(RedisScanReader.MIGRATION_ID, "file-it-" + System.nanoTime());
        scan.run();

        TestRunner deserializer = TestRunners.newTestRunner(RedisTypeDeserializer.class);
        deserializer.setAllowSynchronousSessionCommits(true);
        addPool(deserializer, "source-pool", new StandardRedisConnectionPoolService(), url(SOURCE), "STANDALONE");
        deserializer.setProperty(RedisTypeDeserializer.REDIS_CONNECTION_POOL, "source-pool");
        deserializer.setProperty(RedisTypeDeserializer.HASH_FIELD_BATCH_SIZE, "10");
        for (String type : List.of("string", "hash", "list", "set", "zset", "stream")) {
            scan.getFlowFilesForRelationship(type).forEach(f -> deserializer.enqueue(f.toByteArray(), f.getAttributes()));
        }
        deserializer.run(deserializer.getQueueSize().getObjectCount());
        assertEquals(List.of(), deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_FAILURE));

        TestRunner fileWriter = TestRunners.newTestRunner(KeyRecordFileWriter.class);
        fileWriter.setProperty(KeyRecordFileWriter.FILE, file.toString());
        fileWriter.setProperty(KeyRecordFileWriter.FORMAT, format);
        List<MockFlowFile> records = deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_SUCCESS);
        records.forEach(f -> fileWriter.enqueue(f.toByteArray(), f.getAttributes()));
        fileWriter.run();
        assertEquals(List.of(), fileWriter.getFlowFilesForRelationship(KeyRecordFileWriter.REL_FAILURE));
        assertEquals(8, fileWriter.getCounterValue("File Records Written"));
    }

    private static void assertSameAsSource(GenericContainer<?> target) {
        try (RedisClient sourceClient = RedisClient.create(url(SOURCE)); StatefulRedisConnection<String, String> s = sourceClient.connect();
             RedisClient targetClient = RedisClient.create(url(target)); StatefulRedisConnection<String, String> t = targetClient.connect()) {
            assertTargetMatchesSource(s.sync(), t.sync(), false);
            assertEquals(s.sync().xpending("st", "g").getCount(), t.sync().xpending("st", "g").getCount(), "pending entries");
            assertEquals(s.sync().xpending("st", "g").getConsumerMessageCount(), t.sync().xpending("st", "g").getConsumerMessageCount());
        }
    }
}
