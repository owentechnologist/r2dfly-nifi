package io.dragonfly.nifi.redis.it;

import io.dragonfly.nifi.redis.processors.RedisBatchWriter;
import io.dragonfly.nifi.redis.processors.RedisScanReader;
import io.dragonfly.nifi.redis.processors.RedisTypeDeserializer;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.FakeDistributedMapCacheClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.IntegerListOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The scan path with the RESTORE probe and the restore_rejected loop, against real containers.
 * Dragonfly 2.x restores Redis 7.4 payloads except hashes with field TTLs, and rejects every
 * Redis 8 payload, so the two sources exercise both sides of every verdict. Requires Docker; run
 * via {@code mvn verify}.
 */
@Testcontainers
class RestoreFastPathIT {

    @Container
    static GenericContainer<?> redis74 = new GenericContainer<>(DockerImageName.parse("redis:7.4")).withExposedPorts(6379);

    @Container
    static GenericContainer<?> redis8 = new GenericContainer<>(DockerImageName.parse("redis:latest")).withExposedPorts(6379);

    @Container
    static GenericContainer<?> dragonfly = new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest"))
            .withExposedPorts(6379);

    private enum RawCommand implements ProtocolKeyword {
        HPEXPIRE, HTTL;

        @Override
        public byte[] getBytes() {
            return name().getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        }
    }

    @Test
    void probeSendsEveryRestorableEncodingThroughRestoreAndTheRestThroughTheTypedRead() throws Exception {
        Counters counters = migrate(redis74, true, false);

        assertTargetMatchesSource(redis74, true);
        // 15 keys: hash:fieldttl (listpackex) is the only one the probe sends to the typed read.
        assertEquals(new Counters(14, 1, 0), counters);
        List<Long> ttl = fieldTtls(dragonfly, "hash:fieldttl");
        assertEquals(1, ttl.size(), "the typed path copies field TTLs");
        assertTrue(ttl.get(0) > 3590 && ttl.get(0) <= 3600, "field TTL " + ttl.get(0));
    }

    @Test
    void aRejectedRestoreFallsBackToTheTypedReadForThatKeyOnly() throws Exception {
        Counters counters = migrate(redis74, false, true);

        // dfly-to-dfly leaves streams on the typed path, which does not copy pending entries yet (WP3).
        assertTargetMatchesSource(redis74, false);
        // dfly-to-dfly DUMPs the 14 non-stream keys without probing; only hash:fieldttl is rejected
        // and comes back, so it is read twice: once for RESTORE and once typed.
        assertEquals(new Counters(14, 2, 1), counters);
    }

    @Test
    void aSourceTheTargetCannotRestoreFromIsCopiedEntirelyWithTheTypedRead() throws Exception {
        Counters counters = migrate(redis8, true, false);

        assertTargetMatchesSource(redis8, false);
        assertEquals(new Counters(0, 15, 0), counters);
    }

    @Test
    void withoutTheLoopConnectionARejectedRestoreGoesToFailureAsBefore() throws Exception {
        seed(redis8);
        TestRunner deserializer = deserializer(redis8, false, true);
        TestRunner writer = writer(false);
        scan(redis8).forEach(f -> deserializer.enqueue(f.toByteArray(), f.getAttributes()));
        deserializer.run();
        deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_SUCCESS).forEach(f -> writer.enqueue(f.toByteArray(), f.getAttributes()));
        writer.run();

        assertEquals(14, writer.getFlowFilesForRelationship(RedisBatchWriter.REL_FAILURE).size());
        assertEquals(List.of(), writer.getFlowFilesForRelationship(RedisBatchWriter.REL_RESTORE_REJECTED));
    }

    private record Counters(long restoreReads, long typedReads, long rejected) {
    }

    private static Counters migrate(GenericContainer<?> source, boolean probe, boolean dflyToDfly) throws Exception {
        seed(source);
        try (Client target = new Client(dragonfly)) {
            target.sync().flushall();
        }

        TestRunner deserializer = deserializer(source, probe, dflyToDfly);
        TestRunner writer = writer(true);
        List<MockFlowFile> toDeserialize = scan(source);
        long rejected = 0;
        boolean firstPass = true;
        while (!toDeserialize.isEmpty()) {
            deserializer.clearTransferState();
            toDeserialize.forEach(f -> deserializer.enqueue(f.toByteArray(), f.getAttributes()));
            deserializer.run(1, false, firstPass);
            firstPass = false;
            assertEquals(List.of(), deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_FAILURE));

            writer.clearTransferState();
            deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_SUCCESS).forEach(f -> writer.enqueue(f.toByteArray(), f.getAttributes()));
            writer.run();
            assertEquals(List.of(), writer.getFlowFilesForRelationship(RedisBatchWriter.REL_FAILURE));
            toDeserialize = new ArrayList<>(writer.getFlowFilesForRelationship(RedisBatchWriter.REL_RESTORE_REJECTED));
            rejected += toDeserialize.size();
        }
        deserializer.run(1, true, false);
        return new Counters(counter(deserializer, "Keys Read for RESTORE"), counter(deserializer, "Keys Read for Typed Write"), rejected);
    }

    private static List<MockFlowFile> scan(GenericContainer<?> source) throws Exception {
        TestRunner scan = TestRunners.newTestRunner(RedisScanReader.class);
        scan.setAllowSynchronousSessionCommits(true);
        addPool(scan, "source-pool", new StandardRedisConnectionPoolService(), source);
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        scan.addControllerService("cursor-cache", cache);
        scan.enableControllerService(cache);
        scan.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "source-pool");
        scan.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cursor-cache");
        scan.setProperty(RedisScanReader.MIGRATION_ID, "restore-it-" + System.nanoTime());
        scan.run();
        List<MockFlowFile> scanned = new ArrayList<>();
        for (String type : List.of("string", "hash", "list", "set", "zset", "stream")) {
            scanned.addAll(scan.getFlowFilesForRelationship(type));
        }
        return scanned;
    }

    private static TestRunner deserializer(GenericContainer<?> source, boolean probe, boolean dflyToDfly) throws Exception {
        TestRunner deserializer = TestRunners.newTestRunner(RedisTypeDeserializer.class);
        deserializer.setAllowSynchronousSessionCommits(true);
        addPool(deserializer, "source-pool", new StandardRedisConnectionPoolService(), source);
        deserializer.setProperty(RedisTypeDeserializer.REDIS_CONNECTION_POOL, "source-pool");
        deserializer.setProperty(RedisTypeDeserializer.DFLY_TO_DFLY, String.valueOf(dflyToDfly));
        if (probe) {
            addPool(deserializer, "target-pool", new StandardDragonflyConnectionPoolService(), dragonfly);
            deserializer.setProperty(RedisTypeDeserializer.RESTORE_PROBE_TARGET, "target-pool");
        }
        return deserializer;
    }

    private static TestRunner writer(boolean loopConnected) throws Exception {
        TestRunner writer = TestRunners.newTestRunner(RedisBatchWriter.class);
        writer.setAllowSynchronousSessionCommits(true);
        addPool(writer, "target-pool", new StandardDragonflyConnectionPoolService(), dragonfly);
        writer.setProperty(RedisBatchWriter.DRAGONFLY_CONNECTION_POOL, "target-pool");
        if (loopConnected) {
            writer.addConnection(RedisBatchWriter.REL_RESTORE_REJECTED);
        }
        return writer;
    }

    private static void seed(GenericContainer<?> source) {
        try (Client client = new Client(source)) {
            RedisCommands<String, String> s = client.sync();
            s.flushall();
            s.set("str:int", "12345");
            s.set("str:embstr", "short");
            s.set("str:raw", "x".repeat(100));
            s.psetex("str:ttl", 3_600_000, "expiring");
            s.rpush("list:small", "a", "b", "c");
            s.rpush("list:big", IntStream.range(0, 300).mapToObj(i -> "e" + i).toArray(String[]::new));
            s.sadd("set:int", "1", "2", "3");
            s.sadd("set:small", "a", "b");
            s.sadd("set:big", IntStream.range(0, 300).mapToObj(i -> "m" + i).toArray(String[]::new));
            s.hset("hash:small", Map.of("f1", "v1", "f2", "v2"));
            Map<String, String> bigHash = new TreeMap<>();
            IntStream.range(0, 300).forEach(i -> bigHash.put("f" + i, "v" + i));
            s.hset("hash:big", bigHash);
            s.hset("hash:fieldttl", Map.of("a", "1", "b", "2"));
            s.dispatch(RawCommand.HPEXPIRE, new IntegerListOutput<>(StringCodec.UTF8),
                    new CommandArgs<>(StringCodec.UTF8).addKey("hash:fieldttl").add(3_600_000).add("FIELDS").add(1).add("a"));
            s.zadd("zset:small", 1.0, "a", 2.5, "b");
            for (int i = 0; i < 300; i++) {
                s.zadd("zset:big", i, "m" + i);
            }
            for (int i = 1; i <= 5; i++) {
                s.xadd("stream", new io.lettuce.core.XAddArgs().id(i + "-0"), Map.of("n", String.valueOf(i)));
            }
            s.xgroupCreate(XReadArgs.StreamOffset.from("stream", "0"), "g1", XGroupCreateArgs.Builder.mkstream(false));
            s.xgroupCreate(XReadArgs.StreamOffset.from("stream", "0"), "g2", XGroupCreateArgs.Builder.mkstream(false));
            s.xreadgroup(Consumer.from("g1", "c1"), XReadArgs.Builder.count(2), XReadArgs.StreamOffset.lastConsumed("stream"));
            s.xreadgroup(Consumer.from("g1", "c2"), XReadArgs.Builder.count(1), XReadArgs.StreamOffset.lastConsumed("stream"));
            s.xreadgroup(Consumer.from("g2", "c3"), XReadArgs.Builder.count(3), XReadArgs.StreamOffset.lastConsumed("stream"));
        }
    }

    private static void assertTargetMatchesSource(GenericContainer<?> source, boolean withPendingEntries) {
        try (Client src = new Client(source); Client tgt = new Client(dragonfly)) {
            List<String> keys = src.sync().keys("*").stream().sorted().toList();
            assertEquals(keys, tgt.sync().keys("*").stream().sorted().toList());
            for (String key : keys) {
                assertEquals(snapshot(src.sync(), key, withPendingEntries), snapshot(tgt.sync(), key, withPendingEntries), key);
            }
        }
    }

    /** Stream snapshots cover entries and, per group, pending ids with their consumer and delivery count. */
    private static Object snapshot(RedisCommands<String, String> c, String key, boolean withPendingEntries) {
        String type = c.type(key);
        Object value = switch (type) {
            case "string" -> c.get(key);
            case "list" -> c.lrange(key, 0, -1);
            case "set" -> c.smembers(key).stream().sorted().toList();
            case "hash" -> new TreeMap<>(c.hgetall(key));
            case "zset" -> c.zrangeWithScores(key, 0, -1).toString();
            case "stream" -> {
                List<Object> parts = new ArrayList<>();
                c.xrange(key, Range.create("-", "+")).forEach(m -> parts.add(m.getId() + "=" + m.getBody()));
                for (String group : withPendingEntries ? List.of("g1", "g2") : List.<String>of()) {
                    c.xpending(key, group, Range.create("-", "+"), io.lettuce.core.Limit.from(100))
                            .forEach(p -> parts.add(group + ":" + p.getId() + "@" + p.getConsumer() + "x" + p.getRedeliveryCount()));
                }
                yield parts;
            }
            default -> "unexpected type " + type;
        };
        return List.of(type, value, c.pttl(key) > 0);
    }

    private static List<Long> fieldTtls(GenericContainer<?> container, String key) {
        try (Client client = new Client(container)) {
            return client.sync().dispatch(RawCommand.HTTL, new IntegerListOutput<>(StringCodec.UTF8),
                    new CommandArgs<>(StringCodec.UTF8).addKey(key).add("FIELDS").add(1).add("a")).stream()
                    .filter(ttl -> ttl > 0).toList();
        }
    }

    private static long counter(TestRunner runner, String name) {
        Long value = runner.getCounterValue(name);
        return value == null ? 0L : value;
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
