package io.dragonfly.nifi.redis.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dragonfly.nifi.redis.processors.MigrationReconciler;
import io.dragonfly.nifi.redis.processors.ModuleTypeHandler;
import io.dragonfly.nifi.redis.processors.RedisDeadLetterWriter;
import io.dragonfly.nifi.redis.processors.RedisScanReader;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.FakeDistributedMapCacheClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every module type a Redis source can hold, through the scan path's module route
 * (RedisScanReader unknown, ModuleTypeHandler, RedisDeadLetterWriter) into Dragonfly. Requires
 * Docker; run via {@code mvn verify}.
 */
class ModuleTypesIT {

    private static final GenericContainer<?> DRAGONFLY =
            new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest")).withExposedPorts(6379);

    static {
        DRAGONFLY.start();
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String MODULE_TYPES = "ReJSON-RL,TopK-TYPE,MBbloom--,MBbloomCF,CMSk-TYPE,TSDB-TYPE,TDIS-TYPE,vectorset";

    private record Raw(String name) implements ProtocolKeyword {
        @Override
        public byte[] getBytes() {
            return name.getBytes(StandardCharsets.US_ASCII);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"redis/redis-stack-server:latest", "redis:latest"})
    void copiesWhatDragonflyCanHoldAndDeadLettersTheRestWithAReason(String image, @TempDir Path deadLetters) throws Exception {
        try (GenericContainer<?> source = new GenericContainer<>(DockerImageName.parse(image)).withExposedPorts(6379)) {
            source.start();
            boolean redis8 = image.equals("redis:latest");
            try (Client src = new Client(source); Client tgt = new Client(DRAGONFLY)) {
                tgt.sync().flushall();
                cli(source, "JSON.SET", "json:1", "$", "{\"n\":1}");
                cli(source, "TOPK.RESERVE", "topk:1", "3");
                cli(source, "TOPK.ADD", "topk:1", "hot");
                cli(source, "BF.ADD", "bloom:1", "x");
                cli(source, "CF.ADD", "cuckoo:1", "x");
                cli(source, "CMS.INITBYDIM", "cms:1", "10", "3");
                cli(source, "TS.ADD", "ts:1", "1", "1");
                cli(source, "TDIGEST.CREATE", "tdigest:1");
                if (redis8) {
                    cli(source, "VADD", "vset:1", "VALUES", "2", "1", "2", "e1");
                }

                TestRunner modules = copyModules(source);
                Map<String, String> reasons = deadLetter(modules, deadLetters);

                assertEquals(List.of("json:1", "topk:1"), modules.getFlowFilesForRelationship(ModuleTypeHandler.REL_SUCCESS).stream()
                        .map(f -> f.getAttribute("redis.key")).sorted().toList());
                assertEquals("{\"n\":1}", tgt.sync().dispatch(new Raw("JSON.GET"), new ValueOutput<>(StringCodec.UTF8),
                        new CommandArgs<>(StringCodec.UTF8).addKey("json:1")));
                assertEquals(List.of("hot"), tgt.sync().dispatch(new Raw("TOPK.LIST"), new ArrayOutput<>(StringCodec.UTF8),
                        new CommandArgs<>(StringCodec.UTF8).addKey("topk:1")));

                Map<String, String> expected = new TreeMap<>();
                expected.put("bloom:1", "no lossless copy path for MBbloom-- on this Dragonfly version; RESTORE was rejected: ");
                expected.put("cuckoo:1", "no lossless copy path for MBbloomCF on this Dragonfly version; RESTORE was rejected: ");
                expected.put("cms:1", "no lossless copy path for CMSk-TYPE on this Dragonfly version; RESTORE was rejected: ");
                expected.put("ts:1", "TSDB-TYPE is not supported by Dragonfly");
                expected.put("tdigest:1", "TDIS-TYPE is not supported by Dragonfly");
                if (redis8) {
                    expected.put("vset:1", "vectorset is not supported by Dragonfly");
                }
                assertEquals(expected.keySet(), reasons.keySet(), "one dead-letter record per key that could not be copied");
                expected.forEach((key, reason) -> assertTrue(reasons.get(key).startsWith(reason), key + ": " + reasons.get(key)));

                // A second attempt, as a reconcile pass would make, replaces each record instead of adding one.
                deadLetter(copyModules(source), deadLetters);
                try (Stream<Path> files = Files.list(deadLetters)) {
                    assertEquals(expected.size(), files.count());
                }

                JsonNode report = reconcile(source);
                assertEquals(0, report.get("differences").asLong(), "keys with a dead-letter record are not drift: " + report);
                assertEquals(2, report.get("keys_checked").asLong(), "the JSON and TopK keys that were copied are still checked");
            }
        }
    }

    private static TestRunner copyModules(GenericContainer<?> source) throws Exception {
        TestRunner scan = TestRunners.newTestRunner(RedisScanReader.class);
        scan.setAllowSynchronousSessionCommits(true);
        addPool(scan, "source-pool", new StandardRedisConnectionPoolService(), source);
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        scan.addControllerService("cache", cache);
        scan.enableControllerService(cache);
        scan.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "source-pool");
        scan.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cache");
        scan.setProperty(RedisScanReader.MIGRATION_ID, "modules-" + System.nanoTime());
        scan.setProperty(RedisScanReader.KEY_TYPE_FILTER, MODULE_TYPES);
        scan.run();

        TestRunner modules = TestRunners.newTestRunner(ModuleTypeHandler.class);
        modules.setAllowSynchronousSessionCommits(true);
        addPool(modules, "source-pool", new StandardRedisConnectionPoolService(), source);
        addPool(modules, "target-pool", new StandardDragonflyConnectionPoolService(), DRAGONFLY);
        modules.setProperty(ModuleTypeHandler.SOURCE_CONNECTION_POOL, "source-pool");
        modules.setProperty(ModuleTypeHandler.TARGET_CONNECTION_POOL, "target-pool");
        modules.addConnection(ModuleTypeHandler.REL_UNSUPPORTED);
        scan.getFlowFilesForRelationship(RedisScanReader.REL_UNKNOWN).forEach(f -> modules.enqueue(f.toByteArray(), f.getAttributes()));
        modules.run();
        assertEquals(List.of(), modules.getFlowFilesForRelationship(ModuleTypeHandler.REL_FAILURE).stream()
                .map(f -> f.getAttribute("redis.key") + ": " + f.getAttribute("redis.failure.reason")).toList());
        return modules;
    }

    private static JsonNode reconcile(GenericContainer<?> source) throws Exception {
        TestRunner reconciler = TestRunners.newTestRunner(MigrationReconciler.class);
        reconciler.setAllowSynchronousSessionCommits(true);
        addPool(reconciler, "source-pool", new StandardRedisConnectionPoolService(), source);
        addPool(reconciler, "target-pool", new StandardDragonflyConnectionPoolService(), DRAGONFLY);
        reconciler.setProperty(MigrationReconciler.SOURCE_CONNECTION_POOL, "source-pool");
        reconciler.setProperty(MigrationReconciler.TARGET_CONNECTION_POOL, "target-pool");
        reconciler.setProperty(MigrationReconciler.MODE, "FULL");
        for (int i = 0; i < 100 && reconciler.getFlowFilesForRelationship(MigrationReconciler.REL_REPORT).isEmpty(); i++) {
            reconciler.run(1, false, i == 0);
        }
        return MAPPER.readTree(reconciler.getFlowFilesForRelationship(MigrationReconciler.REL_REPORT).get(0).toByteArray());
    }

    /** Key to reason, read back from the files RedisDeadLetterWriter wrote. */
    private static Map<String, String> deadLetter(TestRunner modules, Path directory) throws Exception {
        TestRunner writer = TestRunners.newTestRunner(RedisDeadLetterWriter.class);
        writer.setProperty(RedisDeadLetterWriter.OUTPUT_DIRECTORY, directory.toString());
        List<MockFlowFile> unsupported = modules.getFlowFilesForRelationship(ModuleTypeHandler.REL_UNSUPPORTED);
        unsupported.forEach(f -> writer.enqueue(f.toByteArray(), f.getAttributes()));
        writer.run(Math.max(1, unsupported.size()));
        writer.assertAllFlowFilesTransferred(RedisDeadLetterWriter.REL_SUCCESS, unsupported.size());
        Map<String, String> reasons = new TreeMap<>();
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : files.toList()) {
                JsonNode attributes = MAPPER.readTree(file.toFile()).get("attributes");
                reasons.put(attributes.get("redis.key").asText(), attributes.get("redis.incompatible.reason").asText());
            }
        }
        return reasons;
    }

    /** Seeded with redis-cli inside the source, which takes any module reply without a parser per command. */
    private static void cli(GenericContainer<?> source, String... command) throws Exception {
        String[] argv = Stream.concat(Stream.of("redis-cli"), Stream.of(command)).toArray(String[]::new);
        var result = source.execInContainer(argv);
        assertEquals(0, result.getExitCode(), result.getStderr());
        assertTrue(!result.getStdout().startsWith("(error)") && !result.getStdout().startsWith("ERR"), String.join(" ", command) + ": " + result.getStdout());
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
