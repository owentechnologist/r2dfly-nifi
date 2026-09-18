package io.dragonfly.nifi.redis.it;

import io.dragonfly.nifi.redis.processors.RedisBatchWriter;
import io.dragonfly.nifi.redis.processors.RedisScanReader;
import io.dragonfly.nifi.redis.processors.RedisTypeDeserializer;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.FakeDistributedMapCacheClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end round trip through RedisScanReader -> RedisTypeDeserializer -> RedisBatchWriter
 * against real Redis and Dragonfly containers, per plan Phase 2. Requires Docker; run via
 * {@code mvn verify}. Each independent TestRunner is bridged manually (NiFi's TestRunner is
 * scoped to one processor) by re-enqueuing one stage's output relationship as the next stage's
 * input, mirroring how the processors connect in a real NiFi flow.
 */
@Testcontainers
class ScanPhaseIT {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);

    @Container
    static GenericContainer<?> dragonfly = new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest")).withExposedPorts(6379);

    private RedisClient redisSeedClient;
    private StatefulRedisConnection<String, String> redisSeedConnection;
    private RedisClient dragonflyCheckClient;
    private StatefulRedisConnection<String, String> dragonflyCheckConnection;

    @BeforeEach
    void setUp() {
        redisSeedClient = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        redisSeedConnection = redisSeedClient.connect();
        dragonflyCheckClient = RedisClient.create("redis://" + dragonfly.getHost() + ":" + dragonfly.getMappedPort(6379));
        dragonflyCheckConnection = dragonflyCheckClient.connect();
    }

    @AfterEach
    void tearDown() {
        redisSeedConnection.close();
        redisSeedClient.shutdown();
        dragonflyCheckConnection.close();
        dragonflyCheckClient.shutdown();
    }

    private void seedRedis(Consumer<RedisCommands<String, String>> setup) {
        setup.accept(redisSeedConnection.sync());
    }

    private RedisCommands<String, String> dragonflyCheck() {
        return dragonflyCheckConnection.sync();
    }

    @Test
    void migratesAllCoreTypesEndToEnd() throws Exception {
        seedRedis(cmds -> {
            cmds.set("it:string", "hello");
            cmds.hset("it:hash", Map.of("a", "1", "b", "2"));
            cmds.rpush("it:list", "x", "y", "z");
            cmds.sadd("it:set", "m1", "m2", "m3");
            cmds.zadd("it:zset", 1.0, "low");
            cmds.zadd("it:zset", 2.0, "high");
        });

        runFullPipeline(1);

        assertEquals("hello", dragonflyCheck().get("it:string"));
        assertEquals(Map.of("a", "1", "b", "2"), dragonflyCheck().hgetall("it:hash"));
        assertEquals(List.of("x", "y", "z"), dragonflyCheck().lrange("it:list", 0, -1));
        assertEquals(Set.of("m1", "m2", "m3"), Set.copyOf(dragonflyCheck().smembers("it:set")));
        assertEquals(2, dragonflyCheck().zcard("it:zset"));
    }

    @Test
    void reassemblesAChunkedHashAcrossMultipleFlowFiles() throws Exception {
        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 0; i < 25; i++) {
            fields.put("field" + i, "value" + i);
        }
        seedRedis(cmds -> cmds.hset("it:bighash", fields));

        // Hash Field Batch Size below the field count forces RedisTypeDeserializer to chunk,
        // and RedisBatchWriter must re-assemble the chunks before writing.
        runFullPipeline(1, runner -> runner.setProperty(RedisTypeDeserializer.HASH_FIELD_BATCH_SIZE, "10"));

        assertEquals(fields, dragonflyCheck().hgetall("it:bighash"));
    }

    @Test
    void rewritesHashExactlyOnRerunAfterASourceFieldIsRemoved() throws Exception {
        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 0; i < 25; i++) {
            fields.put("field" + i, "value" + i);
        }
        seedRedis(cmds -> cmds.hset("it:bighash-rewrite", fields));

        runFullPipeline(1, runner -> runner.setProperty(RedisTypeDeserializer.HASH_FIELD_BATCH_SIZE, "10"));
        assertEquals(fields, dragonflyCheck().hgetall("it:bighash-rewrite"));

        seedRedis(cmds -> cmds.hdel("it:bighash-rewrite", "field7"));
        Map<String, String> expectedAfterRemoval = new LinkedHashMap<>(fields);
        expectedAfterRemoval.remove("field7");

        // Same HASH_FIELD_BATCH_SIZE boundary (now 10/10/4 chunks) - a stale HSET-only rewrite
        // would leave field7 behind instead of removing it, without losing any of the other 24.
        runFullPipeline(1, runner -> runner.setProperty(RedisTypeDeserializer.HASH_FIELD_BATCH_SIZE, "10"));

        assertEquals(expectedAfterRemoval, dragonflyCheck().hgetall("it:bighash-rewrite"));
    }

    @Test
    void preservesTtlByDefault() throws Exception {
        seedRedis(cmds -> cmds.set("it:ttlkey", "v", io.lettuce.core.SetArgs.Builder.ex(3600)));

        runFullPipeline(1);

        Long ttlMs = dragonflyCheck().pttl("it:ttlkey");
        assertTrue(ttlMs > 0 && ttlMs <= 3_600_000, "TTL should be preserved within the original bound, was " + ttlMs);
    }

    private void runFullPipeline(int partitionCount) throws Exception {
        runFullPipeline(partitionCount, r -> { });
    }

    private void runFullPipeline(int partitionCount, Consumer<TestRunner> deserializerCustomizer) throws Exception {
        TestRunner scanRunner = TestRunners.newTestRunner(RedisScanReader.class);
        // RedisScanReader/RedisTypeDeserializer/RedisBatchWriter all call session.commit()
        // directly, which MockProcessSession rejects by default since nifi-mock 1.14.0 unless
        // opted into explicitly.
        scanRunner.setAllowSynchronousSessionCommits(true);
        StandardRedisConnectionPoolService sourcePool = new StandardRedisConnectionPoolService();
        scanRunner.addControllerService("source-pool", sourcePool);
        scanRunner.setProperty(sourcePool, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        scanRunner.setProperty(sourcePool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        scanRunner.enableControllerService(sourcePool);

        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        scanRunner.addControllerService("cursor-cache", cache);
        scanRunner.enableControllerService(cache);

        scanRunner.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "source-pool");
        scanRunner.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cursor-cache");
        scanRunner.setProperty(RedisScanReader.MIGRATION_ID, "it-migration");
        scanRunner.setProperty(RedisScanReader.PARTITION_COUNT, String.valueOf(partitionCount));
        scanRunner.run();

        TestRunner deserializerRunner = TestRunners.newTestRunner(RedisTypeDeserializer.class);
        deserializerRunner.setAllowSynchronousSessionCommits(true);
        deserializerRunner.addControllerService("source-pool", sourcePool);
        deserializerRunner.setProperty(sourcePool, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        deserializerRunner.setProperty(sourcePool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        deserializerRunner.enableControllerService(sourcePool);
        deserializerRunner.setProperty(RedisTypeDeserializer.REDIS_CONNECTION_POOL, "source-pool");
        deserializerCustomizer.accept(deserializerRunner);

        for (var relationship : List.of(RedisScanReader.REL_STRING, RedisScanReader.REL_HASH, RedisScanReader.REL_LIST,
                RedisScanReader.REL_SET, RedisScanReader.REL_ZSET)) {
            for (MockFlowFile flowFile : scanRunner.getFlowFilesForRelationship(relationship)) {
                deserializerRunner.enqueue(flowFile.toByteArray(), flowFile.getAttributes());
            }
        }
        deserializerRunner.run(Math.max(1, deserializerRunner.getQueueSize().getObjectCount()));

        TestRunner writerRunner = TestRunners.newTestRunner(RedisBatchWriter.class);
        writerRunner.setAllowSynchronousSessionCommits(true);
        StandardDragonflyConnectionPoolService targetPool = new StandardDragonflyConnectionPoolService();
        writerRunner.addControllerService("target-pool", targetPool);
        writerRunner.setProperty(targetPool, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://" + dragonfly.getHost() + ":" + dragonfly.getMappedPort(6379));
        writerRunner.setProperty(targetPool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        writerRunner.enableControllerService(targetPool);
        writerRunner.setProperty(RedisBatchWriter.DRAGONFLY_CONNECTION_POOL, "target-pool");

        for (MockFlowFile flowFile : deserializerRunner.getFlowFilesForRelationship(RedisTypeDeserializer.REL_SUCCESS)) {
            writerRunner.enqueue(flowFile.toByteArray(), flowFile.getAttributes());
        }
        writerRunner.run();
    }
}
