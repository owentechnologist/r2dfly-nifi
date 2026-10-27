package io.dragonfly.nifi.redis.it;

import io.dragonfly.nifi.redis.processors.DeleteRedisKey;
import io.dragonfly.nifi.redis.processors.ModuleTypeHandler;
import io.dragonfly.nifi.redis.processors.RedisBatchWriter;
import io.dragonfly.nifi.redis.processors.RedisKeyspaceEventConsumer;
import io.dragonfly.nifi.redis.processors.RedisScanReader;
import io.dragonfly.nifi.redis.processors.RedisSingleKeyFetch;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.LogMessage;
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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The live phase end to end against real containers: RedisKeyspaceEventConsumer -> RedisSingleKeyFetch
 * -> RedisBatchWriter / ModuleTypeHandler / DeleteRedisKey, bridged by hand between TestRunners.
 * The source renames CONFIG away, the way AWS ElastiCache blocks it, and enables notifications on
 * the command line instead. Requires Docker; run via {@code mvn verify}.
 */
@Testcontainers
class LivePhaseIT {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:latest"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--notify-keyspace-events", "KEA", "--rename-command", "CONFIG", "");

    @Container
    static GenericContainer<?> dragonfly = new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest"))
            .withExposedPorts(6379);

    private enum RawCommand implements ProtocolKeyword {
        JSON_SET("JSON.SET"), JSON_GET("JSON.GET"), TOPK_RESERVE("TOPK.RESERVE"), TOPK_ADD("TOPK.ADD");

        private final byte[] bytes;

        RawCommand(String name) {
            this.bytes = name.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }

    private RedisClient sourceClient;
    private StatefulRedisConnection<String, String> sourceConnection;
    private RedisClient targetClient;
    private StatefulRedisConnection<String, String> targetConnection;

    @BeforeEach
    void setUp() {
        sourceClient = RedisClient.create(url(redis));
        sourceConnection = sourceClient.connect();
        targetClient = RedisClient.create(url(dragonfly));
        targetConnection = targetClient.connect();
        sourceConnection.sync().flushall();
        targetConnection.sync().flushall();
    }

    @AfterEach
    void tearDown() {
        sourceConnection.close();
        sourceClient.shutdown();
        targetConnection.close();
        targetClient.shutdown();
    }

    @Test
    void mirrorsFilteredChangesJsonAndRenamesFromAConfigBlockedSource() throws Exception {
        RedisCommands<String, String> source = sourceConnection.sync();
        RedisCommands<String, String> target = targetConnection.sync();
        // As if the scan phase had already copied it before the rename happened.
        target.set("user:old", "renamed");

        TestRunner consumer = TestRunners.newTestRunner(RedisKeyspaceEventConsumer.class);
        addPool(consumer, "source-pool", new StandardRedisConnectionPoolService(), redis);
        consumer.setProperty(RedisKeyspaceEventConsumer.REDIS_CONNECTION_POOL, "source-pool");
        consumer.setProperty(RedisScanReader.PREFIX_DENY_LIST, "user:tmp:");
        consumer.setProperty(RedisScanReader.PREFIX_ONLY_LIST, "user:");
        consumer.run(1, false, true);
        awaitPatternSubscription(source);

        source.set("user:1", "v1", new SetArgs().ex(1000));
        source.hset("user:h", Map.of("f1", "a", "f2", "b"));
        source.set("user:tmp:1", "denied by prefix");
        source.set("order:1", "outside the only-list");
        source.sadd("user:set", "excluded by type");
        source.dispatch(RawCommand.JSON_SET, new StatusOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey("user:doc").add("$").add("{\"a\":1}"));
        source.dispatch(RawCommand.JSON_SET, new StatusOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey("user:doc").add("$.a").add("2"));
        source.dispatch(RawCommand.TOPK_RESERVE, new StatusOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey("user:topk").add(5));
        source.dispatch(RawCommand.TOPK_ADD, new ArrayOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey("user:topk").addValue("hot"));
        source.set("user:old", "renamed");
        source.rename("user:old", "user:new");

        drainEvents(consumer);
        consumer.run(1, true, false);

        TestRunner fetch = TestRunners.newTestRunner(RedisSingleKeyFetch.class);
        addPool(fetch, "source-pool", new StandardRedisConnectionPoolService(), redis);
        fetch.setProperty(RedisSingleKeyFetch.REDIS_CONNECTION_POOL, "source-pool");
        fetch.setProperty(RedisSingleKeyFetch.KEY_TYPE_FILTER, "string,hash,ReJSON-RL,TopK-TYPE");
        forward(consumer, RedisKeyspaceEventConsumer.REL_KEY_CHANGED, fetch);
        fetch.run(Math.max(1, fetch.getQueueSize().getObjectCount()));
        assertEquals(List.of(), fetch.getFlowFilesForRelationship(RedisSingleKeyFetch.REL_FAILURE));

        TestRunner writer = TestRunners.newTestRunner(RedisBatchWriter.class);
        writer.setAllowSynchronousSessionCommits(true);
        addPool(writer, "target-pool", new StandardDragonflyConnectionPoolService(), dragonfly);
        writer.setProperty(RedisBatchWriter.DRAGONFLY_CONNECTION_POOL, "target-pool");
        forward(fetch, RedisSingleKeyFetch.REL_SUCCESS, writer);
        writer.run();
        assertEquals(List.of(), writer.getFlowFilesForRelationship(RedisBatchWriter.REL_FAILURE));

        TestRunner modules = TestRunners.newTestRunner(ModuleTypeHandler.class);
        modules.setAllowSynchronousSessionCommits(true);
        addPool(modules, "source-pool", new StandardRedisConnectionPoolService(), redis);
        addPool(modules, "target-pool", new StandardDragonflyConnectionPoolService(), dragonfly);
        modules.setProperty(ModuleTypeHandler.SOURCE_CONNECTION_POOL, "source-pool");
        modules.setProperty(ModuleTypeHandler.TARGET_CONNECTION_POOL, "target-pool");
        forward(fetch, RedisSingleKeyFetch.REL_MODULE_TYPE, modules);
        modules.run();
        assertEquals(List.of(), modules.getFlowFilesForRelationship(ModuleTypeHandler.REL_FAILURE));

        TestRunner delete = TestRunners.newTestRunner(DeleteRedisKey.class);
        addPool(delete, "target-pool", new StandardDragonflyConnectionPoolService(), dragonfly);
        delete.setProperty(DeleteRedisKey.DRAGONFLY_CONNECTION_POOL, "target-pool");
        forward(consumer, RedisKeyspaceEventConsumer.REL_KEY_DELETED, delete);
        forward(fetch, RedisSingleKeyFetch.REL_KEY_MISSING, delete);
        delete.run(Math.max(1, delete.getQueueSize().getObjectCount()));
        assertEquals(List.of(), delete.getFlowFilesForRelationship(DeleteRedisKey.REL_FAILURE));

        // user:topk is absent by design of the source, not of this flow: Redis 8 publishes no keyspace
        // event for TOPK.*, BF.*, CF.* or CMS.* (JSON.* and TS.* do publish), so no notification-based
        // live phase can see those changes. If this starts failing, Redis began announcing them.
        assertEquals(Set.of("user:1", "user:h", "user:doc", "user:new"), Set.copyOf(target.keys("*")));
        assertEquals("v1", target.get("user:1"));
        long ttlMs = target.pttl("user:1");
        assertTrue(ttlMs > 990_000 && ttlMs <= 1_000_000, "user:1 kept its TTL on the target, got " + ttlMs);
        assertEquals(Map.of("f1", "a", "f2", "b"), target.hgetall("user:h"));
        assertEquals("renamed", target.get("user:new"));
        assertEquals("{\"a\":2}", target.dispatch(RawCommand.JSON_GET, new ValueOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey("user:doc")));
        assertEquals(2L, consumer.getCounterValue("Keyspace Events Skipped (Prefix Filter)"));
        assertEquals(1L, fetch.getCounterValue("Keys Skipped by Type Filter (set)"));
        assertTrue(consumer.getLogger().getWarnMessages().stream().map(LogMessage::getMsg)
                        .anyMatch(msg -> msg.contains("Unable to read notify-keyspace-events")),
                "a blocked CONFIG is reported as a warning, not a start failure");
    }

    private static String url(GenericContainer<?> container) {
        return "redis://" + container.getHost() + ":" + container.getMappedPort(6379);
    }

    private static void addPool(TestRunner runner, String id, ControllerService pool, GenericContainer<?> container) throws Exception {
        runner.addControllerService(id, pool);
        runner.setProperty(pool, AbstractRedisConnectionPoolService.CONNECTION_STRING, url(container));
        runner.setProperty(pool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        runner.enableControllerService(pool);
    }

    /** psubscribe is asynchronous; writes made before the server registers it are never announced. */
    private static void awaitPatternSubscription(RedisCommands<String, String> source) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (source.pubsubNumpat() < 1) {
            assertTrue(System.currentTimeMillis() < deadline, "keyspace pattern subscription never became active");
            Thread.sleep(50);
        }
    }

    /** One event per trigger; keeps triggering until a full second passes with no new event. */
    private static void drainEvents(TestRunner consumer) throws InterruptedException {
        long lastProgressAt = System.currentTimeMillis();
        long seen = handled(consumer);
        while (System.currentTimeMillis() - lastProgressAt < 1_000) {
            consumer.run(1, false, false);
            long now = handled(consumer);
            if (now != seen) {
                seen = now;
                lastProgressAt = System.currentTimeMillis();
            }
            Thread.sleep(5);
        }
    }

    private static long handled(TestRunner consumer) {
        Long skipped = consumer.getCounterValue("Keyspace Events Skipped (Prefix Filter)");
        return consumer.getFlowFilesForRelationship(RedisKeyspaceEventConsumer.REL_KEY_CHANGED).size()
                + consumer.getFlowFilesForRelationship(RedisKeyspaceEventConsumer.REL_KEY_DELETED).size()
                + (skipped == null ? 0 : skipped);
    }

    private static void forward(TestRunner from, Relationship relationship, TestRunner to) {
        for (MockFlowFile flowFile : from.getFlowFilesForRelationship(relationship)) {
            to.enqueue(flowFile.toByteArray(), flowFile.getAttributes());
        }
    }
}
