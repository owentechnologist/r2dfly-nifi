package io.dragonfly.nifi.redis.it;

import io.dragonfly.nifi.redis.processors.ModuleTypeHandler;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression coverage for the TOPK.RESERVE-on-existing-key fix in
 * {@link ModuleTypeHandler#reconstructTopKBatch}: a second write of the same TopK key (e.g. a
 * re-run/reconciliation replay) must succeed rather than erroring on RESERVE against an
 * already-populated target key. Uses two real Dragonfly containers - plain {@code redis:7} has no
 * TopK module, and Dragonfly's native TOPK.* support is exercised here directly (no typed Lettuce
 * command interface covers it, so seeding and verification both dispatch raw commands the same
 * way {@code RawModuleCommands} does in production).
 */
@Testcontainers
class ModuleTypeHandlerIT {

    @Container
    static GenericContainer<?> sourceDragonfly =
            new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest")).withExposedPorts(6379);

    @Container
    static GenericContainer<?> targetDragonfly =
            new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest")).withExposedPorts(6379);

    private enum RawCommand implements ProtocolKeyword {
        TOPK_RESERVE("TOPK.RESERVE"), TOPK_INCRBY("TOPK.INCRBY"), TOPK_LIST("TOPK.LIST");

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
        sourceClient = RedisClient.create("redis://" + sourceDragonfly.getHost() + ":" + sourceDragonfly.getMappedPort(6379));
        sourceConnection = sourceClient.connect();
        targetClient = RedisClient.create("redis://" + targetDragonfly.getHost() + ":" + targetDragonfly.getMappedPort(6379));
        targetConnection = targetClient.connect();
    }

    @AfterEach
    void tearDown() {
        sourceConnection.close();
        sourceClient.shutdown();
        targetConnection.close();
        targetClient.shutdown();
    }

    @Test
    void reRunSucceedsAgainstAnAlreadyPopulatedTargetTopKKey() throws Exception {
        RedisCommands<String, String> source = sourceConnection.sync();
        source.dispatch(RawCommand.TOPK_RESERVE, new StatusOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey("srckey").add(50).add(2000).add(7).add(0.925));
        // TOPK.ADD undercounts a repeated item within (or across) calls against real Dragonfly -
        // verified directly, a known quirk this project's own TopK migration tooling works around
        // by using TOPK.INCRBY for exact counts instead, so seeding does the same here.
        source.dispatch(RawCommand.TOPK_INCRBY, new StatusOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey("srckey").addValue("itemA").add(2).addValue("itemB").add(1));
        Map<String, Long> expectedCounts = Map.of("itemA", 2L, "itemB", 1L);

        TestRunner runner = TestRunners.newTestRunner(ModuleTypeHandler.class);
        // ModuleTypeHandler.onTrigger calls session.commit() directly (per NiFi's own batching
        // pattern), which MockProcessSession rejects by default since nifi-mock 1.14.0 unless
        // opted into explicitly.
        runner.setAllowSynchronousSessionCommits(true);
        StandardRedisConnectionPoolService sourcePool = new StandardRedisConnectionPoolService();
        runner.addControllerService("source-pool", sourcePool);
        runner.setProperty(sourcePool, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://" + sourceDragonfly.getHost() + ":" + sourceDragonfly.getMappedPort(6379));
        runner.setProperty(sourcePool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        runner.enableControllerService(sourcePool);

        StandardDragonflyConnectionPoolService targetPool = new StandardDragonflyConnectionPoolService();
        runner.addControllerService("target-pool", targetPool);
        runner.setProperty(targetPool, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://" + targetDragonfly.getHost() + ":" + targetDragonfly.getMappedPort(6379));
        runner.setProperty(targetPool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        runner.enableControllerService(targetPool);

        runner.setProperty(ModuleTypeHandler.SOURCE_CONNECTION_POOL, "source-pool");
        runner.setProperty(ModuleTypeHandler.TARGET_CONNECTION_POOL, "target-pool");

        runner.enqueue(new byte[0], Map.of("redis.key", "srckey", "redis.type", "TopK-TYPE"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ModuleTypeHandler.REL_SUCCESS, 1);
        assertEquals(expectedCounts, readTargetTopkCounts("srckey"));
        runner.clearTransferState();

        // Re-run against the same key: before the fix, TOPK.RESERVE on this already-populated
        // target key errors and this FlowFile routes to REL_FAILURE instead.
        runner.enqueue(new byte[0], Map.of("redis.key", "srckey", "redis.type", "TopK-TYPE"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ModuleTypeHandler.REL_SUCCESS, 1);
        assertEquals(expectedCounts, readTargetTopkCounts("srckey"));
    }

    private Map<String, Long> readTargetTopkCounts(String key) {
        RedisCommands<String, String> target = targetConnection.sync();
        List<Object> raw = target.dispatch(RawCommand.TOPK_LIST, new ArrayOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey(key).add("WITHCOUNT"));
        Map<String, Long> counts = new LinkedHashMap<>();
        for (int i = 0; i + 1 < raw.size(); i += 2) {
            counts.put(String.valueOf(raw.get(i)), Long.parseLong(String.valueOf(raw.get(i + 1))));
        }
        return counts;
    }
}
