package io.dragonfly.nifi.redis.it;

import io.dragonfly.nifi.redis.processors.RedisBatchWriter;
import io.dragonfly.nifi.redis.processors.RedisScanReader;
import io.dragonfly.nifi.redis.processors.RedisSingleKeyFetch;
import io.dragonfly.nifi.redis.processors.RedisTypeDeserializer;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.FakeDistributedMapCacheClient;
import io.lettuce.core.Consumer;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.models.stream.PendingMessage;
import io.lettuce.core.output.IntegerListOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stream consumer-group state and hash field TTLs through the typed path, from the two sources
 * whose DUMP payloads Dragonfly 2.0.2 rejects outright, so RESTORE cannot carry them. Requires
 * Docker; run via {@code mvn verify}.
 */
class StreamHashFidelityIT {

    private static final GenericContainer<?> DRAGONFLY =
            new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest")).withExposedPorts(6379);

    static {
        DRAGONFLY.start();
    }

    private enum RawCommand implements ProtocolKeyword {
        HEXPIRE, HTTL;

        @Override
        public byte[] getBytes() {
            return name().getBytes(StandardCharsets.US_ASCII);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"redis:latest", "valkey/valkey:latest"})
    void copiesConsumerGroupStateAndFieldTtlsWithoutRestore(String image) throws Exception {
        try (GenericContainer<?> source = new GenericContainer<>(DockerImageName.parse(image)).withExposedPorts(6379)) {
            source.start();
            seed(source);
            try (Client target = new Client(DRAGONFLY)) {
                target.sync().flushall();
            }

            copyWithScan(source);

            try (Client src = new Client(source); Client tgt = new Client(DRAGONFLY)) {
                RedisCommands<String, String> s = src.sync();
                RedisCommands<String, String> t = tgt.sync();
                assertEquals(s.keys("*").stream().sorted().toList(), t.keys("*").stream().sorted().toList());
                for (String stream : List.of("orders", "drained")) {
                    assertEquals(streamState(s, stream), streamState(t, stream), stream);
                    for (String group : groupNames(s, stream)) {
                        assertPendingMatches(s, t, stream, group);
                    }
                }
                assertFieldTtlsMatch(s, t, "h", List.of("a", "b", "c"));
                assertFieldTtlsMatch(s, t, "big", IntStream.range(0, 50).mapToObj(i -> "f" + i).toList());
                assertEquals(new TreeMap<>(s.hgetall("big")), new TreeMap<>(t.hgetall("big")));

                t.del("h");
            }
            copyWithLiveFetch(source, "h");
            try (Client src = new Client(source); Client tgt = new Client(DRAGONFLY)) {
                assertFieldTtlsMatch(src.sync(), tgt.sync(), "h", List.of("a", "b", "c"));
            }
        }
    }

    private static void seed(GenericContainer<?> source) throws InterruptedException {
        try (Client client = new Client(source)) {
            RedisCommands<String, String> s = client.sync();
            s.flushall();
            for (int i = 1; i <= 6; i++) {
                s.xadd("orders", new XAddArgs().id(i + "-0"), Map.of("n", String.valueOf(i)));
            }
            s.xgroupCreate(XReadArgs.StreamOffset.from("orders", "0"), "g1", XGroupCreateArgs.Builder.mkstream(false));
            s.xgroupCreate(XReadArgs.StreamOffset.from("orders", "0"), "g2", XGroupCreateArgs.Builder.mkstream(false));
            s.xreadgroup(Consumer.from("g1", "c1"), XReadArgs.Builder.count(2), XReadArgs.StreamOffset.lastConsumed("orders"));
            Thread.sleep(1500);
            s.xreadgroup(Consumer.from("g1", "c2"), XReadArgs.Builder.count(1), XReadArgs.StreamOffset.lastConsumed("orders"));
            s.xreadgroup(Consumer.from("g2", "c3"), XReadArgs.Builder.count(4), XReadArgs.StreamOffset.lastConsumed("orders"));
            // A second delivery of the same entry raises its delivery count to 2.
            s.xclaim("orders", Consumer.from("g2", "c3"), 0, "2-0");
            s.xack("orders", "g2", "4-0");
            s.xgroupCreateconsumer("orders", Consumer.from("g2", "c4-idle"));
            s.xdel("orders", "6-0");

            s.xadd("drained", new XAddArgs().id("5-0"), Map.of("n", "x"));
            s.xgroupCreate(XReadArgs.StreamOffset.from("drained", "$"), "g", XGroupCreateArgs.Builder.mkstream(false));
            s.xdel("drained", "5-0");

            s.hset("h", Map.of("a", "1", "b", "2", "c", "3"));
            hexpire(s, "h", 3600, "a");
            hexpire(s, "h", 120, "b");
            Map<String, String> big = new LinkedHashMap<>();
            IntStream.range(0, 50).forEach(i -> big.put("f" + i, "v" + i));
            s.hset("big", big);
            hexpire(s, "big", 600, IntStream.range(0, 50).filter(i -> i % 3 == 0).mapToObj(i -> "f" + i).toArray(String[]::new));
        }
    }

    private static void copyWithScan(GenericContainer<?> source) throws Exception {
        TestRunner scan = TestRunners.newTestRunner(RedisScanReader.class);
        scan.setAllowSynchronousSessionCommits(true);
        addPool(scan, "source-pool", new StandardRedisConnectionPoolService(), source);
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        scan.addControllerService("cursor-cache", cache);
        scan.enableControllerService(cache);
        scan.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "source-pool");
        scan.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cursor-cache");
        scan.setProperty(RedisScanReader.MIGRATION_ID, "fidelity-it-" + System.nanoTime());
        scan.run();

        TestRunner deserializer = TestRunners.newTestRunner(RedisTypeDeserializer.class);
        deserializer.setAllowSynchronousSessionCommits(true);
        addPool(deserializer, "source-pool", new StandardRedisConnectionPoolService(), source);
        deserializer.setProperty(RedisTypeDeserializer.REDIS_CONNECTION_POOL, "source-pool");
        deserializer.setProperty(RedisTypeDeserializer.HASH_FIELD_BATCH_SIZE, "10");
        for (String type : List.of("hash", "stream")) {
            scan.getFlowFilesForRelationship(type).forEach(f -> deserializer.enqueue(f.toByteArray(), f.getAttributes()));
        }
        deserializer.run();
        assertEquals(List.of(), deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_FAILURE));
        write(deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_SUCCESS));
    }

    private static void copyWithLiveFetch(GenericContainer<?> source, String key) throws Exception {
        TestRunner fetch = TestRunners.newTestRunner(RedisSingleKeyFetch.class);
        addPool(fetch, "source-pool", new StandardRedisConnectionPoolService(), source);
        fetch.setProperty(RedisSingleKeyFetch.REDIS_CONNECTION_POOL, "source-pool");
        fetch.enqueue(new byte[0], Map.of("redis.key", key));
        fetch.run();
        write(fetch.getFlowFilesForRelationship(RedisSingleKeyFetch.REL_SUCCESS));
    }

    private static void write(List<MockFlowFile> flowFiles) throws Exception {
        TestRunner writer = TestRunners.newTestRunner(RedisBatchWriter.class);
        writer.setAllowSynchronousSessionCommits(true);
        addPool(writer, "target-pool", new StandardDragonflyConnectionPoolService(), DRAGONFLY);
        writer.setProperty(RedisBatchWriter.DRAGONFLY_CONNECTION_POOL, "target-pool");
        flowFiles.forEach(f -> writer.enqueue(f.toByteArray(), f.getAttributes()));
        writer.run();
        assertEquals(List.of(), writer.getFlowFilesForRelationship(RedisBatchWriter.REL_FAILURE));
        assertEquals(List.of(), writer.getFlowFilesForRelationship(RedisBatchWriter.REL_RETRY),
                () -> writer.getLogger().getWarnMessages().stream().map(m -> m.getMsg() + " " + java.util.Arrays.toString(m.getArgs())).toList().toString());
    }

    /**
     * entries-added and max-deleted-entry-id are left out: Dragonfly rejects the XSETID options
     * that would set them, so they count only what the target itself received. lag derives from
     * entries-added, so it is left out with them.
     */
    private static Map<String, Object> streamState(RedisCommands<String, String> c, String key) {
        Map<String, Object> info = flat(c.xinfoStream(key));
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("length", info.get("length"));
        state.put("last-generated-id", info.get("last-generated-id"));
        state.put("entries", c.xrange(key, Range.create("-", "+")).stream().map(m -> m.getId() + "=" + m.getBody()).toList());
        for (Object g : c.xinfoGroups(key)) {
            Map<String, Object> group = flat(g);
            String name = String.valueOf(group.get("name"));
            Map<String, Object> consumers = new TreeMap<>();
            for (Object co : c.xinfoConsumers(key, name)) {
                Map<String, Object> consumer = flat(co);
                consumers.put(String.valueOf(consumer.get("name")), consumer.get("pending"));
            }
            state.put("group " + name, List.of(group.get("last-delivered-id"), String.valueOf(group.get("entries-read")),
                    group.get("pending"), consumers));
        }
        return state;
    }

    private static List<String> groupNames(RedisCommands<String, String> c, String key) {
        return c.xinfoGroups(key).stream().map(g -> String.valueOf(flat(g).get("name"))).toList();
    }

    /** Idle time keeps growing on both sides, so it may differ by the time between the two reads. */
    private static void assertPendingMatches(RedisCommands<String, String> s, RedisCommands<String, String> t, String key, String group) {
        List<PendingMessage> want = s.xpending(key, group, Range.create("-", "+"), Limit.from(1000));
        List<PendingMessage> got = t.xpending(key, group, Range.create("-", "+"), Limit.from(1000));
        assertEquals(want.stream().map(p -> p.getId() + "@" + p.getConsumer() + "x" + p.getRedeliveryCount()).toList(),
                got.stream().map(p -> p.getId() + "@" + p.getConsumer() + "x" + p.getRedeliveryCount()).toList(), key + "/" + group);
        for (int i = 0; i < want.size(); i++) {
            long drift = Math.abs(want.get(i).getMsSinceLastDelivery() - got.get(i).getMsSinceLastDelivery());
            assertTrue(drift < 1000, key + "/" + group + " " + want.get(i).getId() + " idle differs by " + drift + " ms");
        }
    }

    /** HEXPIRE has one-second granularity and both reads happen at slightly different moments. */
    private static void assertFieldTtlsMatch(RedisCommands<String, String> s, RedisCommands<String, String> t, String key, List<String> fields) {
        List<Long> want = httl(s, key, fields);
        List<Long> got = httl(t, key, fields);
        assertEquals(want.size(), got.size());
        for (int i = 0; i < want.size(); i++) {
            String field = fields.get(i);
            if (want.get(i) < 0) {
                assertEquals(want.get(i), got.get(i), key + "." + field + " has no TTL on the source");
            } else {
                assertTrue(Math.abs(want.get(i) - got.get(i)) <= 1, key + "." + field + " TTL " + want.get(i) + " vs " + got.get(i));
            }
        }
    }

    private static void hexpire(RedisCommands<String, String> c, String key, long seconds, String... fields) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).addKey(key).add(seconds).add("FIELDS").add(fields.length);
        for (String f : fields) {
            args.add(f);
        }
        c.dispatch(RawCommand.HEXPIRE, new IntegerListOutput<>(StringCodec.UTF8), args);
    }

    private static List<Long> httl(RedisCommands<String, String> c, String key, List<String> fields) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).addKey(key).add("FIELDS").add(fields.size());
        fields.forEach(args::add);
        return c.dispatch(RawCommand.HTTL, new IntegerListOutput<>(StringCodec.UTF8), args);
    }

    /** XINFO replies come back as a flat key/value list or as a map, depending on the protocol. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> flat(Object reply) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (reply instanceof Map<?, ?> m) {
            m.forEach((k, v) -> map.put(String.valueOf(k), v));
        } else {
            List<Object> list = (List<Object>) reply;
            for (int i = 0; i + 1 < list.size(); i += 2) {
                map.put(String.valueOf(list.get(i)), list.get(i + 1));
            }
        }
        return map;
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
