package io.dragonfly.nifi.redis.it;

import io.dragonfly.nifi.redis.processors.RedisKeyExistenceRouter;
import io.dragonfly.nifi.redis.processors.RedisScanReader;
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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Drives the reverse reconciliation leg against real containers: a RedisScanReader over the
 * Dragonfly target feeds RedisKeyExistenceRouter, which checks each key against the source Redis.
 * Requires Docker; run via {@code mvn verify}.
 */
@Testcontainers
class RedisKeyExistenceRouterIT {

    private static final String PREFIX = "mig";

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);

    @Container
    static GenericContainer<?> dragonfly = new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest")).withExposedPorts(6379);

    private RedisClient sourceClient;
    private StatefulRedisConnection<String, String> sourceConnection;
    private RedisClient targetClient;
    private StatefulRedisConnection<String, String> targetConnection;

    @BeforeEach
    void setUp() {
        sourceClient = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        sourceConnection = sourceClient.connect();
        targetClient = RedisClient.create("redis://" + dragonfly.getHost() + ":" + dragonfly.getMappedPort(6379));
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
    void routesEachTargetKeyOnWhetherTheSourceStillHasIt() throws Exception {
        RedisCommands<String, String> source = sourceConnection.sync();
        RedisCommands<String, String> target = targetConnection.sync();

        source.set("user:1", "still-here");
        target.set(PREFIX + ":user:1", "still-here");

        target.set(PREFIX + ":user:2", "deleted-on-source-while-nobody-watched");

        // Absent from the source as well, so reaching 'filtered' can only come from the deny list
        // and not from the key happening to exist.
        target.set(PREFIX + ":cache:9", "out-of-scope");

        TestRunner router = routerOverTargetScan();

        List<MockFlowFile> exists = router.getFlowFilesForRelationship(RedisKeyExistenceRouter.REL_EXISTS);
        assertEquals(1, exists.size());
        exists.get(0).assertAttributeEquals("redis.key", PREFIX + ":user:1");

        List<MockFlowFile> missing = router.getFlowFilesForRelationship(RedisKeyExistenceRouter.REL_MISSING);
        assertEquals(1, missing.size());
        missing.get(0).assertAttributeEquals("redis.key", "user:2");
        assertEquals(1L, router.getCounterValue("Orphan Keys Found"));

        List<MockFlowFile> filtered = router.getFlowFilesForRelationship(RedisKeyExistenceRouter.REL_FILTERED);
        assertEquals(1, filtered.size());
        filtered.get(0).assertAttributeEquals("redis.key", PREFIX + ":cache:9");

        router.assertTransferCount(RedisKeyExistenceRouter.REL_FAILURE, 0);
    }

    @Test
    void filtersTargetKeysThatDoNotCarryTheMigrationPrefix() throws Exception {
        targetConnection.sync().set("preexisting:1", "target data this migration never wrote");

        TestRunner router = routerOverTargetScan();

        List<MockFlowFile> filtered = router.getFlowFilesForRelationship(RedisKeyExistenceRouter.REL_FILTERED);
        assertEquals(1, filtered.size());
        filtered.get(0).assertAttributeEquals("redis.key", "preexisting:1");
        router.assertTransferCount(RedisKeyExistenceRouter.REL_MISSING, 0);
    }

    private TestRunner routerOverTargetScan() throws Exception {
        TestRunner scanRunner = TestRunners.newTestRunner(RedisScanReader.class);
        scanRunner.setAllowSynchronousSessionCommits(true);
        StandardDragonflyConnectionPoolService targetPool = new StandardDragonflyConnectionPoolService();
        scanRunner.addControllerService("target-pool", targetPool);
        scanRunner.setProperty(targetPool, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://" + dragonfly.getHost() + ":" + dragonfly.getMappedPort(6379));
        scanRunner.setProperty(targetPool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        scanRunner.enableControllerService(targetPool);

        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        scanRunner.addControllerService("cursor-cache", cache);
        scanRunner.enableControllerService(cache);

        scanRunner.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "target-pool");
        scanRunner.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cursor-cache");
        scanRunner.setProperty(RedisScanReader.MIGRATION_ID, "reverse-leg-" + System.nanoTime());
        scanRunner.setProperty(RedisScanReader.PARTITION_COUNT, "1");
        scanRunner.run();

        TestRunner routerRunner = TestRunners.newTestRunner(RedisKeyExistenceRouter.class);
        StandardRedisConnectionPoolService sourcePool = new StandardRedisConnectionPoolService();
        routerRunner.addControllerService("source-pool", sourcePool);
        routerRunner.setProperty(sourcePool, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        routerRunner.setProperty(sourcePool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        routerRunner.enableControllerService(sourcePool);
        routerRunner.setProperty(RedisKeyExistenceRouter.REDIS_CONNECTION_POOL, "source-pool");
        routerRunner.setProperty(RedisKeyExistenceRouter.KEY_PREFIX, PREFIX);
        routerRunner.setProperty(RedisKeyExistenceRouter.PREFIX_DENY_LIST, "cache:");

        for (MockFlowFile flowFile : scanRunner.getFlowFilesForRelationship(RedisScanReader.REL_STRING)) {
            routerRunner.enqueue(flowFile.toByteArray(), flowFile.getAttributes());
        }
        routerRunner.run(Math.max(1, routerRunner.getQueueSize().getObjectCount()));
        return routerRunner;
    }
}
