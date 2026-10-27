package io.dragonfly.nifi.redis.it;

import io.dragonfly.nifi.redis.processors.DeleteRedisKey;
import io.dragonfly.nifi.redis.processors.ModuleTypeHandler;
import io.dragonfly.nifi.redis.processors.RedisBatchWriter;
import io.dragonfly.nifi.redis.processors.RedisReplicationReader;
import io.dragonfly.nifi.redis.processors.RedisTypeDeserializer;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.lettuce.core.Range;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.testcontainers.containers.GenericContainer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The replication copy flow (reader, deserializer, writer, module handler, delete), bridged by hand between TestRunners, one round per pump step. */
final class ReplicationPipeline {

    enum RawCommand implements ProtocolKeyword {
        JSON_SET("JSON.SET"), JSON_GET("JSON.GET");

        private final byte[] bytes;

        RawCommand(String name) {
            bytes = name.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }


    final TestRunner reader;
    final TestRunner deserializer;
    final TestRunner writer;
    final TestRunner modules;
    final TestRunner delete;
    final AtomicLong moved = new AtomicLong();
    int maxBatch;
    private boolean started;

    ReplicationPipeline(String sourceUrl, String sourceMode, GenericContainer<?> target, TestRunner reader) throws Exception {
        this.reader = reader;
        deserializer = TestRunners.newTestRunner(RedisTypeDeserializer.class);
        deserializer.setAllowSynchronousSessionCommits(true);
        addPool(deserializer, "source-pool", new StandardRedisConnectionPoolService(), sourceUrl, sourceMode);
        deserializer.setProperty(RedisTypeDeserializer.REDIS_CONNECTION_POOL, "source-pool");
        writer = TestRunners.newTestRunner(RedisBatchWriter.class);
        writer.setAllowSynchronousSessionCommits(true);
        addPool(writer, "target-pool", new StandardDragonflyConnectionPoolService(), url(target), "STANDALONE");
        writer.setProperty(RedisBatchWriter.DRAGONFLY_CONNECTION_POOL, "target-pool");
        modules = TestRunners.newTestRunner(ModuleTypeHandler.class);
        modules.setAllowSynchronousSessionCommits(true);
        addPool(modules, "source-pool", new StandardRedisConnectionPoolService(), sourceUrl, sourceMode);
        addPool(modules, "target-pool", new StandardDragonflyConnectionPoolService(), url(target), "STANDALONE");
        modules.setProperty(ModuleTypeHandler.SOURCE_CONNECTION_POOL, "source-pool");
        modules.setProperty(ModuleTypeHandler.TARGET_CONNECTION_POOL, "target-pool");
        delete = TestRunners.newTestRunner(DeleteRedisKey.class);
        addPool(delete, "target-pool", new StandardDragonflyConnectionPoolService(), url(target), "STANDALONE");
        delete.setProperty(DeleteRedisKey.DRAGONFLY_CONNECTION_POOL, "target-pool");
    }

    void step() {
        reader.run(1, false, !started);
        boolean first = !started;
        started = true;
        List<MockFlowFile> keys = reader.getFlowFilesForRelationship(RedisReplicationReader.REL_KEY_CHANGED);
        maxBatch = Math.max(maxBatch, keys.size());
        moved.addAndGet(keys.size());
        forward(keys, deserializer);
        reader.clearTransferState();
        deserializer.run(Math.max(1, deserializer.getQueueSize().getObjectCount() / 500 + 1), false, first);
        assertEquals(List.of(), reasons(deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_FAILURE)));
        forward(deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_SUCCESS), writer);
        forward(deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_MODULE_TYPE), modules);
        forward(deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_KEY_MISSING), delete);
        deserializer.clearTransferState();
        runAndCheck(writer, RedisBatchWriter.REL_FAILURE, RedisBatchWriter.REL_SUCCESS, RedisBatchWriter.REL_RETRY);
        runAndCheck(modules, ModuleTypeHandler.REL_FAILURE, ModuleTypeHandler.REL_SUCCESS);
        runAndCheck(delete, DeleteRedisKey.REL_FAILURE, DeleteRedisKey.REL_SUCCESS);
    }

    void pumpFor(long ms) {
        long end = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < end) {
            step();
        }
    }

    /** Until two seconds pass with nothing emitted and the reader has saved an offset. */
    void pumpUntilQuiet() throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        long quietSince = System.currentTimeMillis();
        long seen = moved.get();
        while (true) {
            assertTrue(System.currentTimeMillis() < deadline, "the pipeline never went quiet");
            step();
            if (moved.get() != seen) {
                seen = moved.get();
                quietSince = System.currentTimeMillis();
            } else if (System.currentTimeMillis() - quietSince > 2000
                    && reader.getStateManager().getState(Scope.CLUSTER).toMap().keySet().stream().anyMatch(k -> k.startsWith("stream."))) {
                return;
            }
            Thread.sleep(20);
        }
    }

    long counter(String name) {
        Long value = reader.getCounterValue(name);
        return value == null ? 0L : value;
    }

    private static void runAndCheck(TestRunner runner, org.apache.nifi.processor.Relationship failure,
                                    org.apache.nifi.processor.Relationship... ok) {
        if (runner.getQueueSize().getObjectCount() > 0) {
            runner.run(runner.getQueueSize().getObjectCount() / 50 + 1);
        }
        assertEquals(List.of(), reasons(runner.getFlowFilesForRelationship(failure)), runner.getProcessor().getClass().getSimpleName() + " failures");
        runner.clearTransferState();
    }

    private static List<String> reasons(List<MockFlowFile> failed) {
        return failed.stream().map(f -> f.getAttribute("redis.key") + ": " + f.getAttribute("redis.failure.reason")).toList();
    }

    private static void forward(List<MockFlowFile> flowFiles, TestRunner to) {
        flowFiles.forEach(f -> to.enqueue(f.toByteArray(), f.getAttributes()));
    }

    static void assertTargetMatchesSource(RedisClusterCommands<String, String> s, RedisClusterCommands<String, String> t, boolean modules) {
        List<String> keys = s.keys("*").stream().sorted().toList();
        List<String> targetKeys = t.keys("*").stream().sorted().toList();
        assertEquals(List.of(), keys.stream().filter(k -> !targetKeys.contains(k)).toList(), "missing on the target");
        assertEquals(List.of(), targetKeys.stream().filter(k -> !keys.contains(k)).toList(), "only on the target");
        for (String key : keys) {
            assertEquals(snapshot(s, key, modules), snapshot(t, key, modules), key);
        }
    }

    private static Object snapshot(RedisClusterCommands<String, String> c, String key, boolean modules) {
        String type = c.type(key);
        Object value = switch (type) {
            case "string" -> c.get(key);
            case "list" -> c.lrange(key, 0, -1);
            case "set" -> c.smembers(key).stream().sorted().toList();
            case "hash" -> new TreeMap<>(c.hgetall(key));
            case "zset" -> c.zrangeWithScores(key, 0, -1).toString();
            case "stream" -> c.xrange(key, Range.create("-", "+")).stream().map(m -> m.getId() + "=" + m.getBody()).toList();
            case "ReJSON-RL" -> modules ? c.dispatch(RawCommand.JSON_GET, new ValueOutput<>(StringCodec.UTF8),
                    new CommandArgs<>(StringCodec.UTF8).addKey(key)) : null;
            default -> "unexpected type " + type;
        };
        return new ArrayList<>(List.of(type, String.valueOf(value), c.pttl(key) > 0));
    }

    static void addPool(TestRunner runner, String id, ControllerService pool, String url, String mode) throws Exception {
        runner.addControllerService(id, pool);
        runner.setProperty(pool, AbstractRedisConnectionPoolService.CONNECTION_STRING, url);
        runner.setProperty(pool, AbstractRedisConnectionPoolService.CONNECTION_MODE, mode);
        runner.setProperty(pool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        runner.enableControllerService(pool);
    }

    static String url(GenericContainer<?> container) {
        return "redis://" + container.getHost() + ":" + container.getMappedPort(6379);
    }
}
