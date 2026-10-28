package io.dragonfly.nifi.redis.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dragonfly.nifi.redis.processors.MigrationReconciler;
import io.dragonfly.nifi.redis.processors.RedisBatchWriter;
import io.dragonfly.nifi.redis.processors.RedisTypeDeserializer;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.KeyMapping;
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

        long startedAt = System.currentTimeMillis() / 1000;
        Pipeline pipeline = new Pipeline("FULL");
        JsonNode first = pipeline.pass();
        assertEquals(405, first.get("keys_checked").asLong());
        assertEquals(Map.of("missing_on_target", 405L), counts(first), "an empty target is one repair per key");
        assertTargetMatchesSource();
        assertEquals(405L, pipeline.reconciler.getCounterValue("Reconcile Last Pass Differences"));
        assertEquals(null, pipeline.reconciler.getCounterValue("Reconcile Last Clean Full Pass Started (epoch seconds)"));
        assertEquals(0, pipeline.pass().get("differences").asLong(), "a pass after the repair is clean");
        assertEquals(0L, pipeline.reconciler.getCounterValue("Reconcile Last Pass Differences"));
        assertTrue(pipeline.reconciler.getCounterValue("Reconcile Last Clean Full Pass Started (epoch seconds)") >= startedAt);

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
        assertEquals(0L, pipeline.reconciler.getCounterValue("Reconcile Last Clean Full Pass Started (epoch seconds)"),
                "a FULL pass that found differences withdraws the earlier clean one");
        assertTargetMatchesSource();
        assertEquals(0, pipeline.pass().get("differences").asLong());
        assertTrue(pipeline.reconciler.getCounterValue("Reconcile Last Clean Full Pass Started (epoch seconds)") >= startedAt);

        // A FLUSHDB, which no change stream turns into key-level deletes.
        s.flushdb();
        JsonNode afterFlush = pipeline.pass();
        assertEquals("a DBSIZE drop from 406 to 0", afterFlush.get("triggered_by").asText());
        assertEquals(406L, counts(afterFlush).get("extra_on_target"));
        assertEquals(List.of(), t.keys("*"), "the flushed keys are gone from the target too");
    }

    @Test
    void aStreamWithAGroupComparesCleanAfterEntriesWereDeleted() throws Exception {
        RedisCommands<String, String> s = src.sync();
        for (int i = 1; i <= 5; i++) {
            s.xadd("trimmed", new io.lettuce.core.XAddArgs().id(i + "-0"), Map.of("n", String.valueOf(i)));
        }
        s.xgroupCreate(XReadArgs.StreamOffset.from("trimmed", "0"), "g", XGroupCreateArgs.Builder.mkstream(false));
        s.xreadgroup(Consumer.from("g", "c"), XReadArgs.Builder.count(1), XReadArgs.StreamOffset.lastConsumed("trimmed"));
        s.xdel("trimmed", "4-0", "5-0");

        Pipeline pipeline = new Pipeline("FULL");
        pipeline.deserializer.removeProperty(RedisTypeDeserializer.RESTORE_PROBE_TARGET);
        assertEquals(Map.of("missing_on_target", 1L), counts(pipeline.pass()));
        assertEquals(0, pipeline.pass().get("differences").asLong(), "the group's lag cannot match once entries were deleted, so it is not compared");
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

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource(delimiter = '|', value = {
            "NONE            |               | ",
            "ALL_KEYS        |               | legacy:h,legacy:s,other:h,other:s",
            "SPARED_PREFIXES | legacy:       | legacy:h,legacy:s",
            "SPARED_TYPES    | set           | legacy:s,other:s"})
    void targetOnlyKeysAreSparedAsConfigured(String spare, String list, String survivors) throws Exception {
        RedisCommands<String, String> t = tgt.sync();
        t.hset("legacy:h", "f", "v");
        t.sadd("legacy:s", "m");
        t.hset("other:h", "f", "v");
        t.sadd("other:s", "m");
        List<String> expected = survivors == null ? List.of() : List.of(survivors.split(","));

        Pipeline pipeline = new Pipeline("FULL");
        pipeline.reconciler.setProperty(MigrationReconciler.SPARE_TARGET_ONLY_KEYS, spare);
        if (list != null) {
            pipeline.reconciler.setProperty(spare.equals("SPARED_TYPES") ? MigrationReconciler.SPARED_TYPES : MigrationReconciler.SPARED_PREFIXES, list);
        }
        JsonNode report = pipeline.pass();

        assertEquals(4 - expected.size(), report.get("differences").asLong());
        assertEquals(expected, t.keys("*").stream().sorted().toList());
        assertEquals(expected.isEmpty() ? null : (long) expected.size(), pipeline.reconciler.getCounterValue("Reconcile Target-Only Keys Spared"));
        assertEquals(0, pipeline.pass().get("differences").asLong(), "spared keys never make a pass dirty");
    }

    @Test
    void comparesEachKeyUnderItsRenamedNameAndTheTtlStrategy() throws Exception {
        RedisCommands<String, String> s = src.sync();
        RedisCommands<String, String> t = tgt.sync();
        for (int i = 0; i < 20; i++) {
            s.set("user:" + i, "v" + i);
        }
        s.set("user:ttl", "x", SetArgs.Builder.ex(3600));
        s.hset("order:1", Map.of("f", "v"));

        Pipeline pipeline = new Pipeline("FULL");
        for (TestRunner runner : List.of(pipeline.reconciler, pipeline.writer)) {
            runner.setProperty(KeyMapping.KEY_RENAME, "user:=account:");
            runner.setProperty(RedisBatchWriter.TTL_STRATEGY, "RESET");
            runner.setProperty(RedisBatchWriter.TTL_RESET_OFFSET_MS, "600000");
        }
        assertEquals(Map.of("missing_on_target", 22L), counts(pipeline.pass()));
        assertEquals(22, t.dbsize());
        assertEquals(List.of(), t.keys("user:*"));
        assertEquals("v7", t.get("account:7"));
        long ttl = t.pttl("account:ttl");
        assertTrue(ttl > 4_100_000 && ttl <= 4_200_000, "the source's hour plus ten minutes, was " + ttl);
        assertEquals(0, pipeline.pass().get("differences").asLong(), "renamed keys with reset TTLs match");

        s.del("user:2");
        t.set("user:5", "not written by the migration");
        assertEquals(Map.of("extra_on_target", 1L), counts(pipeline.pass()));
        assertEquals(0, t.exists("account:2"));
        assertEquals("not written by the migration", t.get("user:5"), "a key no source key maps to is left alone");
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

    /** Rollback after cutover: Dragonfly is now the source and the old Redis the target. */
    @Test
    void reverseSyncCopiesDragonflyBackToRedis() throws Exception {
        RedisCommands<String, String> d = tgt.sync();
        RedisCommands<String, String> r = src.sync();
        for (int i = 0; i < 100; i++) {
            d.set("str:" + i, "v" + i);
        }
        d.hset("hash:1", Map.of("f", "v", "g", "w"));
        d.rpush("list:1", "a", "b", "c");
        d.sadd("set:1", "a", "b");
        d.zadd("zset:1", 1.5, "a");
        d.set("ttl:1", "x", SetArgs.Builder.ex(3600));
        d.xadd("stream:1", Map.of("n", "1"));
        d.xgroupCreate(XReadArgs.StreamOffset.from("stream:1", "0"), "g", XGroupCreateArgs.Builder.mkstream(false));
        d.xreadgroup(Consumer.from("g", "c"), XReadArgs.StreamOffset.lastConsumed("stream:1"));
        r.set("str:0", "written to Redis before the rollback");
        r.set("gone:1", "deleted on Dragonfly after cutover");

        Pipeline reverse = new Pipeline("FULL", dragonfly, redis);
        reverse.pass();
        assertMatches(d, r);
        assertEquals(1L, r.xpending("stream:1", "g").getCount(), "the consumer group's pending entry came back too");

        d.set("str:1", "x1");
        d.del("str:2");
        d.hset("hash:1", "h", "x");
        d.set("str:new", "created after cutover");
        JsonNode changes = reverse.pass();
        assertEquals(Map.of("value", 1L, "size", 1L, "missing_on_target", 1L, "extra_on_target", 1L), counts(changes));
        assertMatches(d, r);
        assertEquals(0, reverse.pass().get("differences").asLong());
    }

    private static Map<String, Long> counts(JsonNode report) {
        Map<String, Long> counts = new TreeMap<>();
        report.get("differences_by_reason").fields().forEachRemaining(e -> counts.put(e.getKey(), e.getValue().asLong()));
        return counts;
    }

    private void assertTargetMatchesSource() {
        assertMatches(src.sync(), tgt.sync());
    }

    private static void assertMatches(RedisCommands<String, String> s, RedisCommands<String, String> t) {
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
            this(mode, redis, dragonfly);
        }

        Pipeline(String mode, GenericContainer<?> from, GenericContainer<?> to) throws Exception {
            reconciler = TestRunners.newTestRunner(MigrationReconciler.class);
            reconciler.setAllowSynchronousSessionCommits(true);
            addPool(reconciler, "source-pool", new StandardRedisConnectionPoolService(), from);
            addPool(reconciler, "target-pool", new StandardDragonflyConnectionPoolService(), to);
            reconciler.setProperty(MigrationReconciler.SOURCE_CONNECTION_POOL, "source-pool");
            reconciler.setProperty(MigrationReconciler.TARGET_CONNECTION_POOL, "target-pool");
            reconciler.setProperty(MigrationReconciler.MODE, mode);
            reconciler.setProperty(MigrationReconciler.SCAN_COUNT, "50");
            // Every pass in these tests is asked for by pass(), which clears the interval by restarting.
            reconciler.setProperty(MigrationReconciler.PASS_INTERVAL_MS, "1");
            deserializer = TestRunners.newTestRunner(RedisTypeDeserializer.class);
            deserializer.setAllowSynchronousSessionCommits(true);
            addPool(deserializer, "source-pool", new StandardRedisConnectionPoolService(), from);
            addPool(deserializer, "probe-pool", new StandardDragonflyConnectionPoolService(), to);
            deserializer.setProperty(RedisTypeDeserializer.REDIS_CONNECTION_POOL, "source-pool");
            deserializer.setProperty(RedisTypeDeserializer.RESTORE_PROBE_TARGET, "probe-pool");
            deserializer.setProperty(RedisTypeDeserializer.INCLUDE_CONSUMER_GROUPS, "true");
            writer = TestRunners.newTestRunner(RedisBatchWriter.class);
            writer.setAllowSynchronousSessionCommits(true);
            addPool(writer, "target-pool", new StandardDragonflyConnectionPoolService(), to);
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
