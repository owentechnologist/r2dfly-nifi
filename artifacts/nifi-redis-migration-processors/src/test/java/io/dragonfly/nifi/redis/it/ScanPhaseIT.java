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
    void aChunkedKeyThatFailsCarriesItsWholeRecord() throws Exception {
        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 0; i < 25; i++) {
            fields.put("field" + i, "value" + i);
        }
        seedRedis(cmds -> cmds.hset("it:bighash-conflict", fields));
        dragonflyCheck().set("it:bighash-conflict", "already here");

        TestRunner writer = runFullPipeline(1, runner -> runner.setProperty(RedisTypeDeserializer.HASH_FIELD_BATCH_SIZE, "10"),
                runner -> runner.setProperty(RedisBatchWriter.CONFLICT_STRATEGY, "FAIL"));

        MockFlowFile failed = writer.getFlowFilesForRelationship(RedisBatchWriter.REL_FAILURE).stream()
                .filter(f -> "it:bighash-conflict".equals(f.getAttribute("redis.key"))).findFirst().orElseThrow();
        assertEquals(fields, io.dragonfly.nifi.redis.util.RedisTypeSerializer.readJson(failed.toByteArray()).value,
                "the dead-letter record holds the joined hash, not an empty FlowFile");
        assertEquals(null, failed.getAttribute("redis.chunk.index"));
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

    @Test
    void aKeyWhoseTtlCannotBeReadFailsInsteadOfBecomingPersistent() throws Exception {
        seedRedis(cmds -> {
            cmds.set("it:unreadable-ttl", "v", io.lettuce.core.SetArgs.Builder.ex(3600));
            cmds.aclSetuser("nopttl", io.lettuce.core.AclSetuserArgs.Builder.on().addPassword("pw")
                    .allKeys().allCommands().removeCommand(io.lettuce.core.protocol.CommandType.PTTL));
        });

        TestRunner scanRunner = TestRunners.newTestRunner(RedisScanReader.class);
        scanRunner.setAllowSynchronousSessionCommits(true);
        StandardRedisConnectionPoolService sourcePool = new StandardRedisConnectionPoolService();
        scanRunner.addControllerService("source-pool", sourcePool);
        scanRunner.setProperty(sourcePool, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://nopttl:pw@" + redis.getHost() + ":" + redis.getMappedPort(6379));
        scanRunner.setProperty(sourcePool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        scanRunner.enableControllerService(sourcePool);
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        scanRunner.addControllerService("cursor-cache", cache);
        scanRunner.enableControllerService(cache);
        scanRunner.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "source-pool");
        scanRunner.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cursor-cache");
        scanRunner.setProperty(RedisScanReader.MIGRATION_ID, "it-unreadable-ttl");
        scanRunner.setProperty(RedisScanReader.KEY_PATTERN, "it:unreadable-ttl");
        scanRunner.run();

        assertEquals(List.of(), scanRunner.getFlowFilesForRelationship(RedisScanReader.REL_STRING));
        List<MockFlowFile> failed = scanRunner.getFlowFilesForRelationship(RedisScanReader.REL_FAILURE);
        assertEquals(1, failed.size());
        assertTrue(failed.get(0).getAttribute("redis.failure.reason").startsWith("PTTL on the source failed"),
                failed.get(0).getAttribute("redis.failure.reason"));
    }

    @Test
    void aScanWhosePartitionIsTakenOverStopsWithoutEmittingMore() throws Exception {
        seedRedis(cmds -> {
            for (int i = 0; i < 300; i++) {
                cmds.set("it:fenced:" + i, "v");
            }
        });
        io.dragonfly.nifi.redis.util.FakeAtomicMapCacheClient cache = new io.dragonfly.nifi.redis.util.FakeAtomicMapCacheClient() {
            private int heartbeats;

            @Override
            public synchronized <K, V> org.apache.nifi.distributed.cache.client.AtomicCacheEntry<K, V, Long> fetch(K key,
                    org.apache.nifi.distributed.cache.client.Serializer<K> keySerializer,
                    org.apache.nifi.distributed.cache.client.Deserializer<V> valueDeserializer) throws java.io.IOException {
                if (String.valueOf(key).contains(".claim.") && ++heartbeats == 2) {
                    org.apache.nifi.distributed.cache.client.Serializer<String> text = (v, out) -> out.write(v.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    put(String.valueOf(key), "another-task|" + System.currentTimeMillis(), text, text);
                }
                return super.fetch(key, keySerializer, valueDeserializer);
            }
        };
        TestRunner scanRunner = TestRunners.newTestRunner(RedisScanReader.class);
        scanRunner.setAllowSynchronousSessionCommits(true);
        StandardRedisConnectionPoolService sourcePool = new StandardRedisConnectionPoolService();
        scanRunner.addControllerService("source-pool", sourcePool);
        scanRunner.setProperty(sourcePool, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        scanRunner.setProperty(sourcePool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        scanRunner.enableControllerService(sourcePool);
        scanRunner.addControllerService("cursor-cache", cache);
        scanRunner.enableControllerService(cache);
        scanRunner.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "source-pool");
        scanRunner.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cursor-cache");
        scanRunner.setProperty(RedisScanReader.MIGRATION_ID, "it-fenced");
        scanRunner.setProperty(RedisScanReader.KEY_PATTERN, "it:fenced:*");
        scanRunner.setProperty(RedisScanReader.SCAN_COUNT, "20");
        scanRunner.run();

        int emitted = scanRunner.getFlowFilesForRelationship(RedisScanReader.REL_STRING).size();
        assertTrue(emitted > 0 && emitted < 300, "only the pages before the take-over were emitted, was " + emitted);
        assertTrue(!new io.dragonfly.nifi.redis.util.PartitionAssigner(cache, "it-fenced").isPartitionComplete(0), "the partition is left to its new owner");
        seedRedis(cmds -> cmds.del(cmds.keys("it:fenced:*").toArray(new String[0])));
    }

    @Test
    void aTtlIsReducedByTheTimeTheKeySpentInTheFlow() throws Exception {
        seedRedis(cmds -> cmds.set("it:queued-ttl", "v", io.lettuce.core.SetArgs.Builder.px(10_000)));

        runFullPipeline(1, r -> { }, writer -> {
            try {
                Thread.sleep(3_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Long ttlMs = dragonflyCheck().pttl("it:queued-ttl");
        assertTrue(ttlMs > 0 && ttlMs <= 7_500, "the copy expires with the source key, not 3 s later; PTTL was " + ttlMs);
    }

    private void runFullPipeline(int partitionCount) throws Exception {
        runFullPipeline(partitionCount, r -> { });
    }

    private void runFullPipeline(int partitionCount, Consumer<TestRunner> deserializerCustomizer) throws Exception {
        runFullPipeline(partitionCount, deserializerCustomizer, r -> { });
    }

    private TestRunner runFullPipeline(int partitionCount, Consumer<TestRunner> deserializerCustomizer, Consumer<TestRunner> writerCustomizer) throws Exception {
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

        writerCustomizer.accept(writerRunner);
        for (MockFlowFile flowFile : deserializerRunner.getFlowFilesForRelationship(RedisTypeDeserializer.REL_SUCCESS)) {
            writerRunner.enqueue(flowFile.toByteArray(), flowFile.getAttributes());
        }
        writerRunner.run();
        return writerRunner;
    }
}
