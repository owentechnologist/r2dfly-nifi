package io.dragonfly.nifi.redis.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dragonfly.nifi.redis.processors.MigrationReconciler;
import io.dragonfly.nifi.redis.processors.RedisBatchWriter;
import io.dragonfly.nifi.redis.processors.RedisTypeDeserializer;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.lettuce.core.Consumer;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.nifi.controller.ControllerService;
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
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MigrationReconciler finding drift that no change stream reports, and the copy pipeline it feeds
 * (RedisTypeDeserializer then RedisBatchWriter) repairing it, against real containers. Requires
 * Docker; run via {@code mvn verify}.
 */
@Testcontainers
class MigrationReconcilerIT {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4")).withExposedPorts(6379);

    @Container
    static GenericContainer<?> dragonfly = new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest"))
            .withExposedPorts(6379);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Client src;
    private Client tgt;

    @BeforeEach
    void setUp() {
        src = new Client(redis);
        tgt = new Client(dragonfly);
        src.sync().flushall();
        tgt.sync().flushall();
    }

    @AfterEach
    void tearDown() {
        src.close();
        tgt.close();
    }

    @Test
    void findsAndRepairsDriftThatNoChangeStreamSaw() throws Exception {
        RedisCommands<String, String> s = src.sync();
        RedisCommands<String, String> t = tgt.sync();
        for (int i = 0; i < 200; i++) {
            s.set("str:" + i, "v" + (i % 10));
            s.hset("hash:" + i, Map.of("f", "v", "n", String.valueOf(i)));
        }
        s.rpush("list:1", "a", "b");
        s.zadd("zset:1", 1.0, "a");
        s.sadd("set:1", "a", "b");
        s.set("ttl:1", "x", SetArgs.Builder.ex(3600));
        s.xadd("stream:1", Map.of("n", "1"));
        s.xgroupCreate(XReadArgs.StreamOffset.from("stream:1", "0"), "g", XGroupCreateArgs.Builder.mkstream(false));
        s.xreadgroup(Consumer.from("g", "c"), XReadArgs.StreamOffset.lastConsumed("stream:1"));

        Pipeline pipeline = new Pipeline("FULL");
        JsonNode first = pipeline.pass();
        assertEquals(405, first.get("keys_checked").asLong());
        assertEquals(Map.of("missing_on_target", 405L), counts(first), "an empty target is one repair per key");
        assertTargetMatchesSource();
        assertEquals(0, pipeline.pass().get("differences").asLong(), "a pass after the repair is clean");

        // Drift no change stream reports: source writes made while no Live Phase was listening, and
        // writes made straight to the target.
        s.set("str:1", "zz");
        s.hset("hash:1", "added", "1");
        s.set("str:new", "created");
        s.expire("str:3", 60);
        t.set("only:on-target", "x");
        t.hset("hash:2", "f", "X");

        JsonNode drift = pipeline.pass();
        assertEquals(Map.of("value", 2L, "size", 1L, "missing_on_target", 1L, "ttl", 1L, "extra_on_target", 1L), counts(drift));
        assertTargetMatchesSource();
        assertEquals(0, pipeline.pass().get("differences").asLong());

        // A FLUSHDB, which no change stream turns into key-level deletes.
        s.flushdb();
        JsonNode afterFlush = pipeline.pass();
        assertEquals("a DBSIZE drop from 406 to 0", afterFlush.get("triggered_by").asText());
        assertEquals(406L, counts(afterFlush).get("extra_on_target"));
        assertEquals(List.of(), t.keys("*"), "the flushed keys are gone from the target too");
    }

    @Test
    void keysTheMigrationSkipsAreNotDifferences() throws Exception {
        RedisCommands<String, String> s = src.sync();
        s.hset("user:1", Map.of("f", "v"));
        s.set("user:2", "a string, outside the type filter");
        s.hset("order:1", Map.of("f", "v"));

        Pipeline pipeline = new Pipeline("FULL");
        pipeline.reconciler.setProperty(io.dragonfly.nifi.redis.processors.RedisScanReader.PREFIX_ONLY_LIST, "user:");
        pipeline.reconciler.setProperty(io.dragonfly.nifi.redis.processors.RedisSingleKeyFetch.KEY_TYPE_FILTER, "hash");
        JsonNode report = pipeline.pass();

        assertEquals(Map.of("missing_on_target", 1L), counts(report));
        assertEquals(List.of("user:1"), tgt.sync().keys("*"));
        assertEquals(0, pipeline.pass().get("differences").asLong(), "the skipped keys stay out of every pass");
    }

    @Test
    void aDbsizeDropStartsAFullPassEvenInSampleMode() throws Exception {
        RedisCommands<String, String> s = src.sync();
        for (int i = 0; i < 500; i++) {
            s.set("k:" + i, "v");
        }
        Pipeline pipeline = new Pipeline("SAMPLE");
        pipeline.reconciler.setProperty(MigrationReconciler.SAMPLE_SIZE, "50");
        JsonNode sample = pipeline.pass();
        assertEquals("SAMPLE", sample.get("mode").asText());
        assertTrue(sample.get("keys_checked").asLong() > 0 && sample.get("keys_checked").asLong() <= 50, sample.toString());

        pipeline.pumpUntilQuiet(() -> pipeline.reconciler.setProperty(MigrationReconciler.MODE, "FULL"));
        pipeline.reconciler.setProperty(MigrationReconciler.MODE, "SAMPLE");
        assertEquals(500L, tgt.sync().dbsize());

        s.flushdb();
        JsonNode afterDrop = pipeline.pass();
        assertEquals("FULL", afterDrop.get("mode").asText());
        assertTrue(afterDrop.get("triggered_by").asText().startsWith("a DBSIZE drop from 500 to 0"), afterDrop.toString());
        assertEquals(0L, tgt.sync().dbsize());
    }

    private static Map<String, Long> counts(JsonNode report) {
        Map<String, Long> counts = new TreeMap<>();
        report.get("differences_by_reason").fields().forEachRemaining(e -> counts.put(e.getKey(), e.getValue().asLong()));
        return counts;
    }

    private void assertTargetMatchesSource() {
        RedisCommands<String, String> s = src.sync();
        RedisCommands<String, String> t = tgt.sync();
        List<String> keys = s.keys("*").stream().sorted().toList();
        assertEquals(keys, t.keys("*").stream().sorted().toList());
        for (String key : keys) {
            assertEquals(snapshot(s, key), snapshot(t, key), key);
        }
    }

    private static Object snapshot(RedisCommands<String, String> c, String key) {
        String type = c.type(key);
        Object value = switch (type) {
            case "string" -> c.get(key);
            case "list" -> c.lrange(key, 0, -1);
            case "set" -> c.smembers(key).stream().sorted().toList();
            case "hash" -> new TreeMap<>(c.hgetall(key));
            case "zset" -> c.zrangeWithScores(key, 0, -1).toString();
            case "stream" -> c.xrange(key, Range.create("-", "+")).stream().map(m -> m.getId() + "=" + m.getBody()).toList();
            default -> "unexpected type " + type;
        };
        return List.of(type, String.valueOf(value), c.pttl(key) > 0);
    }

    /** The reconciler and the copy flow it feeds, bridged by hand between TestRunners. */
    private static final class Pipeline {
        final TestRunner reconciler;
        final TestRunner deserializer;
        final TestRunner writer;
        private boolean started;

        Pipeline(String mode) throws Exception {
            reconciler = TestRunners.newTestRunner(MigrationReconciler.class);
            reconciler.setAllowSynchronousSessionCommits(true);
            addPool(reconciler, "source-pool", new StandardRedisConnectionPoolService(), redis);
            addPool(reconciler, "target-pool", new StandardDragonflyConnectionPoolService(), dragonfly);
            reconciler.setProperty(MigrationReconciler.SOURCE_CONNECTION_POOL, "source-pool");
            reconciler.setProperty(MigrationReconciler.TARGET_CONNECTION_POOL, "target-pool");
            reconciler.setProperty(MigrationReconciler.MODE, mode);
            reconciler.setProperty(MigrationReconciler.SCAN_COUNT, "50");
            // Every pass in these tests is asked for by pass(), which clears the interval by restarting.
            reconciler.setProperty(MigrationReconciler.PASS_INTERVAL_MS, "1");
            deserializer = TestRunners.newTestRunner(RedisTypeDeserializer.class);
            deserializer.setAllowSynchronousSessionCommits(true);
            addPool(deserializer, "source-pool", new StandardRedisConnectionPoolService(), redis);
            deserializer.setProperty(RedisTypeDeserializer.REDIS_CONNECTION_POOL, "source-pool");
            writer = TestRunners.newTestRunner(RedisBatchWriter.class);
            writer.setAllowSynchronousSessionCommits(true);
            addPool(writer, "target-pool", new StandardDragonflyConnectionPoolService(), dragonfly);
            writer.setProperty(RedisBatchWriter.DRAGONFLY_CONNECTION_POOL, "target-pool");
        }

        /** Runs the reconciler until it reports one finished pass, repairing as it goes; returns that report. */
        JsonNode pass() throws Exception {
            long deadline = System.currentTimeMillis() + 60_000;
            while (true) {
                assertTrue(System.currentTimeMillis() < deadline, "no report within a minute");
                reconciler.run(1, false, !started);
                started = true;
                List<MockFlowFile> reports = reconciler.getFlowFilesForRelationship(MigrationReconciler.REL_REPORT);
                repair();
                if (!reports.isEmpty()) {
                    JsonNode report = MAPPER.readTree(reports.get(0).toByteArray());
                    reconciler.clearTransferState();
                    return report;
                }
            }
        }

        void pumpUntilQuiet(Runnable configure) throws Exception {
            configure.run();
            while (pass().get("differences").asLong() > 0) {
                // each pass repairs what it found; the loop ends on the first clean one
            }
        }

        private void repair() {
            List<MockFlowFile> repairs = reconciler.getFlowFilesForRelationship(MigrationReconciler.REL_REPAIR);
            if (repairs.isEmpty()) {
                return;
            }
            repairs.forEach(f -> deserializer.enqueue(f.toByteArray(), f.getAttributes()));
            reconciler.clearTransferState();
            deserializer.run(deserializer.getQueueSize().getObjectCount() / 500 + 1);
            assertEquals(List.of(), deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_FAILURE));
            deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_SUCCESS).forEach(f -> writer.enqueue(f.toByteArray(), f.getAttributes()));
            deserializer.clearTransferState();
            writer.run(writer.getQueueSize().getObjectCount() / 50 + 1);
            assertEquals(List.of(), writer.getFlowFilesForRelationship(RedisBatchWriter.REL_FAILURE));
            writer.clearTransferState();
        }
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

        RedisCommands<String, String> sync() {
            return connection.sync();
        }

        @Override
        public void close() {
            connection.close();
            client.shutdown();
        }
    }
}
