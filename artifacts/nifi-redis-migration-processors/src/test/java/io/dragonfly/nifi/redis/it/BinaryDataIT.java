package io.dragonfly.nifi.redis.it;

import io.dragonfly.nifi.redis.processors.KeyRecordFileReader;
import io.dragonfly.nifi.redis.processors.KeyRecordFileWriter;
import io.dragonfly.nifi.redis.processors.MigrationReconciler;
import io.dragonfly.nifi.redis.processors.RedisBatchWriter;
import io.dragonfly.nifi.redis.processors.RedisReplicationReader;
import io.dragonfly.nifi.redis.processors.RedisScanReader;
import io.dragonfly.nifi.redis.processors.RedisTypeDeserializer;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.FakeDistributedMapCacheClient;
import io.dragonfly.nifi.redis.util.KeyMapping;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import static io.dragonfly.nifi.redis.it.ReplicationPipeline.addPool;
import static io.dragonfly.nifi.redis.it.ReplicationPipeline.url;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Key names and values that are not UTF-8 text reach Dragonfly byte for byte, on every path: the
 * typed copy, the RESTORE fast path, a JSON-lines file, the replication live phase, the reconciler
 * and a prefix rename. Requires Docker; run via {@code mvn verify}.
 */
class BinaryDataIT {

    private static final GenericContainer<?> SOURCE = new GenericContainer<>(DockerImageName.parse("redis:7.4")).withExposedPorts(6379);
    private static final GenericContainer<?> DRAGONFLY =
            new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest")).withExposedPorts(6379);

    private static final byte[] BINARY_KEY = {(byte) 0xff, 0x01, 'k'};
    private static final byte[] BINARY = {0x00, (byte) 0xff, (byte) 0xfe, (byte) 0x80, 'v'};

    @TempDir
    Path dir;

    private static RedisClient sourceClient;
    private static RedisClient targetClient;
    private static StatefulRedisConnection<byte[], byte[]> sourceConnection;
    private static StatefulRedisConnection<byte[], byte[]> targetConnection;

    @BeforeAll
    static void start() {
        SOURCE.start();
        DRAGONFLY.start();
        sourceClient = RedisClient.create(url(SOURCE));
        targetClient = RedisClient.create(url(DRAGONFLY));
        sourceConnection = sourceClient.connect(ByteArrayCodec.INSTANCE);
        targetConnection = targetClient.connect(ByteArrayCodec.INSTANCE);
    }

    @AfterAll
    static void stop() {
        sourceConnection.close();
        targetConnection.close();
        sourceClient.shutdown();
        targetClient.shutdown();
        SOURCE.stop();
        DRAGONFLY.stop();
    }

    @BeforeEach
    void seed() {
        RedisCommands<byte[], byte[]> s = sourceConnection.sync();
        s.flushall();
        targetConnection.sync().flushall();
        s.set(BINARY_KEY, bytes("a text value under a binary name"));
        s.set(bytes("s:binary-value"), BINARY);
        s.hset(bytes("h"), Map.of(BINARY, BINARY, bytes("plain"), bytes("text")));
        s.rpush(bytes("l"), BINARY, bytes("text"), BINARY);
        s.sadd(bytes("set"), BINARY, bytes("text"));
        s.zadd(bytes("z"), ScoredValue.just(1.5, BINARY), ScoredValue.just(2, bytes("text")));
        s.xadd(bytes("st"), new XAddArgs().id("1-0"), Map.of(BINARY, BINARY));
        s.set(join(bytes("user:"), BINARY), bytes("renamed"));
    }

    @Test
    void theTypedCopyKeepsEveryByte() throws Exception {
        write(scanAndRead(d -> { }), w -> { });
        assertSameAsSource(KeyMapping.identity());
    }

    @Test
    void theRestoreFastPathKeepsEveryByte() throws Exception {
        write(scanAndRead(d -> {
            try {
                addPool(d, "probe-pool", new StandardDragonflyConnectionPoolService(), url(DRAGONFLY), "STANDALONE");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            d.setProperty(RedisTypeDeserializer.RESTORE_PROBE_TARGET, "probe-pool");
        }), w -> { });
        assertSameAsSource(KeyMapping.identity());
    }

    @Test
    void aJsonLinesFileKeepsEveryByte() throws Exception {
        Path file = dir.resolve("binary.jsonl");
        TestRunner fileWriter = TestRunners.newTestRunner(KeyRecordFileWriter.class);
        fileWriter.setProperty(KeyRecordFileWriter.FILE, file.toString());
        scanAndRead(d -> { }).forEach(f -> fileWriter.enqueue(f.toByteArray(), f.getAttributes()));
        fileWriter.run();
        assertTrue(java.nio.file.Files.readString(file).contains("\"base64\":true"), "binary values travel as base64");

        TestRunner reader = TestRunners.newTestRunner(KeyRecordFileReader.class);
        reader.setProperty(KeyRecordFileReader.FILE, file.toString());
        reader.run();
        write(reader.getFlowFilesForRelationship(KeyRecordFileReader.REL_SUCCESS), w -> { });
        assertSameAsSource(KeyMapping.identity());
    }

    @Test
    void aPrefixRenameMatchesTheBytesOfABinaryKey() throws Exception {
        write(scanAndRead(d -> { }), w -> w.setProperty(KeyMapping.KEY_RENAME, "user:=account:"));
        assertEquals("renamed", new String(targetConnection.sync().get(join(bytes("account:"), BINARY)), StandardCharsets.UTF_8));
        assertSameAsSource(KeyMapping.of("user:=account:", null, null));
    }

    @Test
    void theReconcilerComparesAndRepairsBinaryValues() throws Exception {
        write(scanAndRead(d -> { }), w -> { });
        TestRunner reconciler = reconciler();
        assertEquals(0, differences(reconciler), "binary keys and values compare equal once copied");

        targetConnection.sync().set(BINARY_KEY, BINARY);
        targetConnection.sync().hset(bytes("h"), BINARY, bytes("drifted"));
        reconciler.clearTransferState();
        assertEquals(2, differences(reconciler));
        List<MockFlowFile> repairs = reconciler.getFlowFilesForRelationship(MigrationReconciler.REL_REPAIR);
        write(read(repairs, d -> { }), w -> { });
        assertSameAsSource(KeyMapping.identity());
    }

    @Test
    void theReplicationLivePhaseKeepsEveryByte() throws Exception {
        TestRunner reader = TestRunners.newTestRunner(RedisReplicationReader.class);
        reader.setAllowSynchronousSessionCommits(true);
        addPool(reader, "source-pool", new StandardRedisConnectionPoolService(), url(SOURCE), "STANDALONE");
        reader.setProperty(RedisReplicationReader.REDIS_CONNECTION_POOL, "source-pool");
        ReplicationPipeline pipeline = new ReplicationPipeline(url(SOURCE), "STANDALONE", DRAGONFLY, reader);
        pipeline.pumpUntilQuiet();

        RedisCommands<byte[], byte[]> s = sourceConnection.sync();
        s.set(join(BINARY_KEY, bytes(":live")), BINARY);
        s.hset(bytes("h"), BINARY, bytes("changed live"));
        s.del(bytes("s:binary-value"));
        Thread.sleep(500);
        pipeline.pumpUntilQuiet();
        assertSameAsSource(KeyMapping.identity());
        pipeline.reader.run(1, true, false);
    }

    @Test
    void streamGroupAndConsumerNamesKeepEveryByte() throws Exception {
        RedisCommands<byte[], byte[]> s = sourceConnection.sync();
        // No zero byte in the consumer name: Dragonfly cuts a consumer name at the first one (verified behavior).
        byte[] consumer = join(bytes("c-"), new byte[] {(byte) 0xff, (byte) 0xfe, (byte) 0x80, 'v'});
        s.xadd(bytes("sg"), new XAddArgs().id("1-0"), Map.of(bytes("f"), bytes("v")));
        s.xadd(bytes("sg"), new XAddArgs().id("2-0"), Map.of(bytes("f"), bytes("w")));
        s.xgroupCreate(io.lettuce.core.XReadArgs.StreamOffset.from(bytes("sg"), "0"), BINARY);
        s.xreadgroup(io.lettuce.core.Consumer.from(BINARY, consumer), io.lettuce.core.XReadArgs.Builder.count(1),
                io.lettuce.core.XReadArgs.StreamOffset.lastConsumed(bytes("sg")));

        write(read(List.of(streamFlowFile("sg")), d -> d.setProperty(RedisTypeDeserializer.INCLUDE_CONSUMER_GROUPS, "true")), w -> { });

        RedisCommands<byte[], byte[]> t = targetConnection.sync();
        assertEquals(groupNames(s, "sg"), groupNames(t, "sg"));
        assertEquals(List.of(hex(BINARY)), groupNames(t, "sg"));
        assertEquals(List.of(hex(consumer)), t.xinfoConsumers(bytes("sg"), BINARY).stream().map(c -> hex(field(c, "name"))).toList());
        assertEquals(List.of("1-0 " + hex(consumer)), pendingConsumers(s, "sg", BINARY), "the source's pending entry, read raw");
        assertEquals(pendingConsumers(s, "sg", BINARY), pendingConsumers(t, "sg", BINARY));
    }

    private static MockFlowFile streamFlowFile(String key) {
        MockFlowFile flowFile = new MockFlowFile(1);
        flowFile.putAttributes(Map.of("redis.key", key, "redis.type", "stream"));
        return flowFile;
    }

    private static List<String> groupNames(RedisCommands<byte[], byte[]> c, String key) {
        return c.xinfoGroups(bytes(key)).stream().map(g -> hex(field(g, "name"))).toList();
    }

    /** "id consumer-hex" for each pending entry, read with the raw XPENDING reply so the consumer keeps its bytes. */
    private static List<String> pendingConsumers(RedisCommands<byte[], byte[]> c, String key, byte[] group) {
        List<Object> reply = c.dispatch(io.lettuce.core.protocol.CommandType.XPENDING,
                new io.lettuce.core.output.ArrayOutput<>(io.lettuce.core.codec.ByteArrayCodec.INSTANCE),
                new io.lettuce.core.protocol.CommandArgs<>(io.lettuce.core.codec.ByteArrayCodec.INSTANCE)
                        .addKey(bytes(key)).addValue(group).add("-").add("+").add(10));
        return reply.stream().map(e -> (List<?>) e).map(e -> new String((byte[]) e.get(0), java.nio.charset.StandardCharsets.US_ASCII) + " " + hex((byte[]) e.get(1))).toList();
    }

    /** A field of an XINFO reply entry, which Lettuce gives as a flat list or a map depending on the protocol. */
    private static byte[] field(Object entry, String name) {
        if (entry instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (name.equals(new String((byte[]) e.getKey(), java.nio.charset.StandardCharsets.US_ASCII))) {
                    return (byte[]) e.getValue();
                }
            }
        }
        List<?> flat = (List<?>) entry;
        for (int i = 0; i + 1 < flat.size(); i += 2) {
            if (name.equals(new String((byte[]) flat.get(i), java.nio.charset.StandardCharsets.US_ASCII))) {
                return (byte[]) flat.get(i + 1);
            }
        }
        throw new AssertionError("no " + name + " in " + entry);
    }

    private static List<MockFlowFile> scanAndRead(Consumer<TestRunner> deserializerSetup) throws Exception {
        TestRunner scan = TestRunners.newTestRunner(RedisScanReader.class);
        scan.setAllowSynchronousSessionCommits(true);
        addPool(scan, "source-pool", new StandardRedisConnectionPoolService(), url(SOURCE), "STANDALONE");
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        scan.addControllerService("cursor-cache", cache);
        scan.enableControllerService(cache);
        scan.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "source-pool");
        scan.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cursor-cache");
        scan.setProperty(RedisScanReader.MIGRATION_ID, "binary-" + System.nanoTime());
        scan.run();
        List<MockFlowFile> scanned = new java.util.ArrayList<>();
        for (String type : List.of("string", "hash", "list", "set", "zset", "stream")) {
            scanned.addAll(scan.getFlowFilesForRelationship(type));
        }
        assertEquals(8, scanned.size());
        return read(scanned, deserializerSetup);
    }

    private static List<MockFlowFile> read(List<MockFlowFile> flowFiles, Consumer<TestRunner> setup) throws Exception {
        TestRunner deserializer = TestRunners.newTestRunner(RedisTypeDeserializer.class);
        deserializer.setAllowSynchronousSessionCommits(true);
        addPool(deserializer, "source-pool", new StandardRedisConnectionPoolService(), url(SOURCE), "STANDALONE");
        deserializer.setProperty(RedisTypeDeserializer.REDIS_CONNECTION_POOL, "source-pool");
        setup.accept(deserializer);
        flowFiles.forEach(f -> deserializer.enqueue(f.toByteArray(), f.getAttributes()));
        deserializer.run(Math.max(1, deserializer.getQueueSize().getObjectCount()));
        assertEquals(List.of(), deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_FAILURE));
        return deserializer.getFlowFilesForRelationship(RedisTypeDeserializer.REL_SUCCESS);
    }

    private static void write(List<MockFlowFile> records, Consumer<TestRunner> setup) throws Exception {
        TestRunner writer = TestRunners.newTestRunner(RedisBatchWriter.class);
        writer.setAllowSynchronousSessionCommits(true);
        addPool(writer, "target-pool", new StandardDragonflyConnectionPoolService(), url(DRAGONFLY), "STANDALONE");
        writer.setProperty(RedisBatchWriter.DRAGONFLY_CONNECTION_POOL, "target-pool");
        setup.accept(writer);
        records.forEach(f -> writer.enqueue(f.toByteArray(), f.getAttributes()));
        writer.run();
        assertEquals(List.of(), writer.getFlowFilesForRelationship(RedisBatchWriter.REL_FAILURE));
    }

    private static TestRunner reconciler() throws Exception {
        TestRunner reconciler = TestRunners.newTestRunner(MigrationReconciler.class);
        reconciler.setAllowSynchronousSessionCommits(true);
        addPool(reconciler, "source-pool", new StandardRedisConnectionPoolService(), url(SOURCE), "STANDALONE");
        addPool(reconciler, "target-pool", new StandardDragonflyConnectionPoolService(), url(DRAGONFLY), "STANDALONE");
        reconciler.setProperty(MigrationReconciler.SOURCE_CONNECTION_POOL, "source-pool");
        reconciler.setProperty(MigrationReconciler.TARGET_CONNECTION_POOL, "target-pool");
        reconciler.setProperty(MigrationReconciler.MODE, "FULL");
        reconciler.setProperty(MigrationReconciler.PASS_INTERVAL_MS, "1");
        return reconciler;
    }

    private static long differences(TestRunner reconciler) throws Exception {
        for (int i = 0; i < 50; i++) {
            reconciler.run(1, false, i == 0);
            List<MockFlowFile> reports = reconciler.getFlowFilesForRelationship(MigrationReconciler.REL_REPORT);
            if (!reports.isEmpty()) {
                return Long.parseLong(reports.get(0).getAttribute("reconcile.differences"));
            }
        }
        throw new AssertionError("no reconcile report");
    }

    /** Every source key, under its mapped name on the target, with the same type and the same bytes. */
    private static void assertSameAsSource(KeyMapping mapping) {
        RedisCommands<byte[], byte[]> s = sourceConnection.sync();
        RedisCommands<byte[], byte[]> t = targetConnection.sync();
        Map<String, String> expected = new TreeMap<>();
        for (byte[] key : s.keys(bytes("*"))) {
            expected.put(hex(mapping.toTarget(key).orElseThrow()), snapshot(s, key));
        }
        Map<String, String> actual = new TreeMap<>();
        for (byte[] key : t.keys(bytes("*"))) {
            actual.put(hex(key), snapshot(t, key));
        }
        assertEquals(expected, actual);
    }

    private static String snapshot(RedisCommands<byte[], byte[]> c, byte[] key) {
        String type = c.type(key);
        return type + " " + switch (type) {
            case "string" -> hex(c.get(key));
            case "hash" -> new TreeMap<>(c.hgetall(key).entrySet().stream()
                    .collect(java.util.stream.Collectors.toMap(e -> hex(e.getKey()), e -> hex(e.getValue())))).toString();
            case "list" -> c.lrange(key, 0, -1).stream().map(BinaryDataIT::hex).toList().toString();
            case "set" -> c.smembers(key).stream().map(BinaryDataIT::hex).sorted().toList().toString();
            case "zset" -> c.zrangeWithScores(key, 0, -1).stream().map(v -> hex(v.getValue()) + "=" + v.getScore()).toList().toString();
            case "stream" -> c.xrange(key, Range.create("-", "+")).stream().map(m -> m.getId() + m.getBody().entrySet().stream()
                    .map(e -> hex(e.getKey()) + "=" + hex(e.getValue())).sorted().toList()).toList().toString();
            default -> "unexpected";
        };
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] join(byte[] a, byte[] b) {
        byte[] joined = java.util.Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, joined, a.length, b.length);
        return joined;
    }
}
