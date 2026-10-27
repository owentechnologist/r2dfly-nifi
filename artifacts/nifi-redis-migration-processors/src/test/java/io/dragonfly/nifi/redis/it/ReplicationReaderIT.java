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
import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RedisReplicationReader feeding the copy pipeline (RedisTypeDeserializer, RedisBatchWriter,
 * ModuleTypeHandler, DeleteRedisKey), against real sources and a Dragonfly target, while the
 * source keeps taking writes. Requires Docker; run via {@code mvn verify}.
 */
class ReplicationReaderIT {

    private static final GenericContainer<?> DRAGONFLY =
            new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest")).withExposedPorts(6379);

    static {
        DRAGONFLY.start();
    }

    private enum RawCommand implements ProtocolKeyword {
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

    @ParameterizedTest
    @ValueSource(strings = {"redis:6-alpine", "redis:7.4", "redis:latest", "valkey/valkey:latest"})
    void copiesASnapshotTakenUnderWritesAndResumesAfterACrashWithAPartialResync(String image) throws Exception {
        try (GenericContainer<?> source = source(image); Client src = new Client(source); Client tgt = new Client(DRAGONFLY)) {
            boolean modules = image.equals("redis:latest");
            seed(src.sync(), modules);
            tgt.sync().flushall();

            Pipeline pipeline = new Pipeline(source, reader(source));
            Writes writes = new Writes(source, modules, 1);
            writes.start();
            pipeline.pumpFor(4000);
            writes.stopAndJoin();
            pipeline.pumpUntilQuiet();
            assertTargetMatchesSource(src.sync(), tgt.sync(), modules);
            assertEquals(1L, pipeline.counter("Replication Full Resyncs"));

            // A crash: the state is whatever the reader last saved while writes were still arriving.
            Writes moreWrites = new Writes(source, modules, 2);
            moreWrites.start();
            pipeline.pumpFor(1500);
            Map<String, String> savedAtCrash = pipeline.reader.getStateManager().getState(Scope.CLUSTER).toMap();
            pipeline.reader.run(1, true, false);
            moreWrites.stopAndJoin();
            src.sync().incr("counter:after-crash");
            src.sync().del("str:0");

            TestRunner restarted = reader(source);
            restarted.getStateManager().setState(savedAtCrash, Scope.CLUSTER);
            Pipeline resumed = new Pipeline(source, restarted);
            resumed.pumpUntilQuiet();
            assertTargetMatchesSource(src.sync(), tgt.sync(), modules);
            assertEquals(1L, resumed.counter("Replication Partial Resyncs"));
            assertEquals(0L, resumed.counter("Replication Full Resyncs"), "the restart resumes instead of copying everything again");
            resumed.reader.run(1, true, false);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"redis:7.4", "valkey/valkey:latest"})
    void keepsReadingWhileTheFlowIsBlockedAndHoldsOnlyTheDistinctChangedKeys(String image) throws Exception {
        try (GenericContainer<?> source = source(image); Client src = new Client(source); Client tgt = new Client(DRAGONFLY)) {
            seed(src.sync(), false);
            tgt.sync().flushall();
            Pipeline pipeline = new Pipeline(source, reader(source));
            pipeline.pumpUntilQuiet();
            pipeline.maxBatch = 0;

            // Nothing triggers the reader while 100k writes land on 50 keys, as under back-pressure.
            RedisCommands<String, String> s = src.sync();
            for (int i = 0; i < 100_000; i++) {
                s.incr("hot:" + (i % 50));
            }
            Thread.sleep(1000);
            assertEquals(0L, pipeline.counter("Replication Disconnects"));
            pipeline.step();
            assertEquals(50L, pipeline.counter("Replication Pending Keys"), "the 50 changed keys wait in the reader");
            assertTrue(pipeline.counter("Replication Lag (seconds)") >= 1, "behind since the burst began");
            assertEquals(0L, pipeline.counter("Replication Caught Up"));
            pipeline.pumpUntilQuiet();
            assertEquals(0L, pipeline.counter("Replication Lag (bytes)"));
            assertEquals(0L, pipeline.counter("Replication Pending Keys"));
            assertEquals(0L, pipeline.counter("Replication Lag (seconds)"), "caught up again");
            assertEquals(1L, pipeline.counter("Replication Caught Up"));

            assertTargetMatchesSource(src.sync(), tgt.sync(), false);
            assertEquals(1L, pipeline.counter("Replication Full Resyncs"), "the replica connection survived the burst");
            assertEquals(0L, pipeline.counter("Replication Disconnects"));
            assertTrue(pipeline.maxBatch <= 100, "pending keys were coalesced, largest batch " + pipeline.maxBatch);

            // A dropped connection, as opposed to a restart: the reader reconnects on its own and continues.
            s.clientKill(io.lettuce.core.KillArgs.Builder.typeSlave());
            for (int i = 0; i < 1000; i++) {
                s.incr("after-kill:" + (i % 10));
            }
            pipeline.pumpUntilQuiet();
            Thread.sleep(2500);
            s.set("after-reconnect", "1");
            pipeline.pumpUntilQuiet();
            assertTargetMatchesSource(src.sync(), tgt.sync(), false);
            assertTrue(pipeline.counter("Replication Disconnects") >= 1, "the kill was noticed");
            assertTrue(pipeline.counter("Replication Partial Resyncs") >= 1, "the reconnect continued the stream");
            assertEquals(1L, pipeline.counter("Replication Full Resyncs"), "no second full copy after the reconnect");
            pipeline.reader.run(1, true, false);
        }
    }

    /**
     * The intent RedisShake cannot express without a Lua script: copy exactly the keys under
     * "user:" that are hashes, in the snapshot and in the live phase, through renames into and out
     * of the prefix, deletes, expiries and a multi-key MSET that mixes both sides.
     */
    @org.junit.jupiter.api.Test
    void copiesExactlyThePrefixAndTypeItWasAskedForInBothPhases() throws Exception {
        try (GenericContainer<?> source = source("redis:7.4"); Client src = new Client(source); Client tgt = new Client(DRAGONFLY)) {
            RedisCommands<String, String> s = src.sync();
            s.flushall();
            tgt.sync().flushall();
            for (int i = 0; i < 10; i++) {
                s.hset("user:" + i, Map.of("n", String.valueOf(i)));
                s.set("user:s" + i, "string, wrong type");
                s.hset("order:" + i, Map.of("n", String.valueOf(i)));
            }
            TestRunner reader = reader(source);
            reader.setProperty(io.dragonfly.nifi.redis.processors.RedisScanReader.PREFIX_ONLY_LIST, "user:");
            Pipeline pipeline = new Pipeline(source, reader);
            pipeline.deserializer.setProperty(io.dragonfly.nifi.redis.processors.RedisSingleKeyFetch.KEY_TYPE_FILTER, "hash");
            pipeline.pumpUntilQuiet();

            s.rename("user:1", "user:renamed-in-prefix");
            s.rename("user:2", "order:renamed-out");
            s.rename("order:3", "user:renamed-into-prefix");
            s.del("user:4");
            s.pexpire("user:5", 200);
            s.mset(Map.of("user:6", "now a string", "order:6", "x"));
            s.hset("user:new", Map.of("n", "new"));
            s.sadd("user:set", "wrong type");
            Thread.sleep(500);
            pipeline.pumpUntilQuiet();

            List<String> expected = s.keys("user:*").stream().filter(k -> s.type(k).equals("hash")).sorted().toList();
            assertEquals(List.of("user:0", "user:3", "user:7", "user:8", "user:9", "user:new", "user:renamed-in-prefix",
                    "user:renamed-into-prefix"), expected);
            assertEquals(expected, tgt.sync().keys("*").stream().sorted().toList());
            for (String key : expected) {
                assertEquals(s.hgetall(key), tgt.sync().hgetall(key), key);
            }
            pipeline.reader.run(1, true, false);
        }
    }

    private static GenericContainer<?> source(String image) {
        GenericContainer<?> source = new GenericContainer<>(DockerImageName.parse(image)).withExposedPorts(6379);
        source.start();
        try (Client client = new Client(source)) {
            client.sync().configSet("repl-diskless-sync-delay", "0");
        }
        return source;
    }

    private static TestRunner reader(GenericContainer<?> source) throws Exception {
        TestRunner reader = TestRunners.newTestRunner(RedisReplicationReader.class);
        reader.setAllowSynchronousSessionCommits(true);
        addPool(reader, "source-pool", new StandardRedisConnectionPoolService(), source);
        reader.setProperty(RedisReplicationReader.REDIS_CONNECTION_POOL, "source-pool");
        reader.setProperty(RedisReplicationReader.SCAN_COUNT, "50");
        return reader;
    }

    private static void seed(RedisCommands<String, String> s, boolean modules) {
        s.flushall();
        for (int i = 0; i < 300; i++) {
            s.set("str:" + i, "v" + i);
            s.hset("hash:" + i, Map.of("a", "1", "b", String.valueOf(i)));
            s.rpush("list:" + (i % 30), "e" + i);
            s.zadd("zset:" + (i % 30), i, "m" + i);
            s.sadd("set:" + (i % 30), "m" + i);
        }
        s.set("ttl:1", "expiring", SetArgs.Builder.ex(3600));
        s.xadd("stream:1", Map.of("n", "0"));
        if (modules) {
            s.dispatch(RawCommand.JSON_SET, new StatusOutput<>(StringCodec.UTF8),
                    new CommandArgs<>(StringCodec.UTF8).addKey("json:1").add("$").add("{\"n\":0}"));
        }
    }

    /** A mix of commands whose replay would corrupt data if applied twice: INCR, LPUSH, XADD. */
    private static final class Writes extends Thread {
        private final GenericContainer<?> source;
        private final boolean modules;
        private final Random random;
        private final AtomicBoolean stop = new AtomicBoolean();

        Writes(GenericContainer<?> source, boolean modules, long seed) {
            this.source = source;
            this.modules = modules;
            this.random = new Random(seed);
        }

        @Override
        public void run() {
            try (Client client = new Client(source)) {
                RedisCommands<String, String> s = client.sync();
                while (!stop.get()) {
                    int n = random.nextInt(300);
                    try {
                        write(s, n);
                    } catch (io.lettuce.core.RedisCommandExecutionException e) {
                        // RENAME of a key an earlier command already renamed or deleted
                        assertTrue(e.getMessage().contains("no such key"), e.getMessage());
                    }
                }
            }
        }

        private void write(RedisCommands<String, String> s, int n) {
                    switch (random.nextInt(10)) {
                        case 0 -> s.incr("counter:" + (n % 20));
                        case 1 -> s.lpush("list:" + (n % 30), "p" + n);
                        case 2 -> s.hset("hash:" + n, "c", String.valueOf(random.nextInt()));
                        case 3 -> s.zincrby("zset:" + (n % 30), 1.5, "m" + n);
                        case 4 -> s.del("str:" + n);
                        case 5 -> s.set("new:" + n, "x", SetArgs.Builder.px(600_000));
                        case 6 -> s.rename("set:" + (n % 30), "renamed:" + n);
                        case 7 -> s.xadd("stream:1", Map.of("n", String.valueOf(n)));
                        case 8 -> s.expire("hash:" + n, 900);
                        default -> {
                            if (modules) {
                                s.dispatch(RawCommand.JSON_SET, new StatusOutput<>(StringCodec.UTF8),
                                        new CommandArgs<>(StringCodec.UTF8).addKey("json:1").add("$.n").add(String.valueOf(n)));
                            } else {
                                s.sadd("set:" + (n % 30), "late" + n);
                            }
                        }
                    }
        }

        void stopAndJoin() throws InterruptedException {
            stop.set(true);
            join();
        }
    }

    /** The copy flow, bridged by hand between TestRunners, one round per pump step. */
    private static final class Pipeline {
        final TestRunner reader;
        final TestRunner deserializer;
        final TestRunner writer;
        final TestRunner modules;
        final TestRunner delete;
        final AtomicLong moved = new AtomicLong();
        int maxBatch;
        private boolean started;

        Pipeline(GenericContainer<?> source, TestRunner reader) throws Exception {
            this.reader = reader;
            deserializer = TestRunners.newTestRunner(RedisTypeDeserializer.class);
            deserializer.setAllowSynchronousSessionCommits(true);
            addPool(deserializer, "source-pool", new StandardRedisConnectionPoolService(), source);
            deserializer.setProperty(RedisTypeDeserializer.REDIS_CONNECTION_POOL, "source-pool");
            writer = TestRunners.newTestRunner(RedisBatchWriter.class);
            writer.setAllowSynchronousSessionCommits(true);
            addPool(writer, "target-pool", new StandardDragonflyConnectionPoolService(), DRAGONFLY);
            writer.setProperty(RedisBatchWriter.DRAGONFLY_CONNECTION_POOL, "target-pool");
            modules = TestRunners.newTestRunner(ModuleTypeHandler.class);
            modules.setAllowSynchronousSessionCommits(true);
            addPool(modules, "source-pool", new StandardRedisConnectionPoolService(), source);
            addPool(modules, "target-pool", new StandardDragonflyConnectionPoolService(), DRAGONFLY);
            modules.setProperty(ModuleTypeHandler.SOURCE_CONNECTION_POOL, "source-pool");
            modules.setProperty(ModuleTypeHandler.TARGET_CONNECTION_POOL, "target-pool");
            delete = TestRunners.newTestRunner(DeleteRedisKey.class);
            addPool(delete, "target-pool", new StandardDragonflyConnectionPoolService(), DRAGONFLY);
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
            assertEquals(List.of(), deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_FAILURE));
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
                        && reader.getStateManager().getState(Scope.CLUSTER).get("offset") != null) {
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
            assertEquals(List.of(), runner.getFlowFilesForRelationship(failure), runner.getProcessor().getClass().getSimpleName() + " failures");
            runner.clearTransferState();
        }

        private static void forward(List<MockFlowFile> flowFiles, TestRunner to) {
            flowFiles.forEach(f -> to.enqueue(f.toByteArray(), f.getAttributes()));
        }
    }

    private static void assertTargetMatchesSource(RedisCommands<String, String> s, RedisCommands<String, String> t, boolean modules) {
        List<String> keys = s.keys("*").stream().sorted().toList();
        List<String> targetKeys = t.keys("*").stream().sorted().toList();
        assertEquals(List.of(), keys.stream().filter(k -> !targetKeys.contains(k)).toList(), "missing on the target");
        assertEquals(List.of(), targetKeys.stream().filter(k -> !keys.contains(k)).toList(), "only on the target");
        for (String key : keys) {
            assertEquals(snapshot(s, key, modules), snapshot(t, key, modules), key);
        }
    }

    private static Object snapshot(RedisCommands<String, String> c, String key, boolean modules) {
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
