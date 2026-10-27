package io.dragonfly.nifi.redis.it;

import io.dragonfly.nifi.redis.processors.RedisScanReader;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.FakeDistributedMapCacheClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * How much SCAN work RedisScanReader puts on a standalone source as its partition count grows,
 * counted on the source itself (INFO commandstats). Requires Docker; run via {@code mvn verify}.
 */
@Testcontainers
class ScanLoadIT {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4")).withExposedPorts(6379);

    @Test
    void scansTheSourceOnceWhateverThePartitionCount() throws Exception {
        try (StatefulRedisConnection<String, String> connection = RedisClient.create(url()).connect()) {
            RedisCommands<String, String> s = connection.sync();
            s.flushall();
            for (int i = 0; i < 2000; i++) {
                s.set("k:" + i, "v");
            }

            // Distinct keys: SCAN promises every key at least once, not exactly once.
            long onePartition = scanCalls(s, () -> assertEquals(2000, new HashSet<>(scan(1)).size()));
            long fourPartitions = scanCalls(s, () -> assertEquals(2000, new HashSet<>(scan(4)).size()));

            assertEquals(onePartition, fourPartitions,
                    "SCAN calls with 4 partitions (" + fourPartitions + ") vs 1 partition (" + onePartition + ")");
        }
    }

    private interface Scan {
        void run() throws Exception;
    }

    private static long scanCalls(RedisCommands<String, String> s, Scan scan) throws Exception {
        s.configResetstat();
        scan.run();
        String stats = s.info("commandstats");
        for (String line : stats.split("\r?\n")) {
            if (line.startsWith("cmdstat_scan:")) {
                return Long.parseLong(line.replaceAll("^cmdstat_scan:calls=(\\d+),.*", "$1"));
            }
        }
        return 0;
    }

    /** One run of the reader with as many tasks as partitions, each claiming its own. */
    private static List<String> scan(int partitions) throws Exception {
        TestRunner runner = TestRunners.newTestRunner(RedisScanReader.class);
        runner.setAllowSynchronousSessionCommits(true);
        StandardRedisConnectionPoolService pool = new StandardRedisConnectionPoolService();
        runner.addControllerService("pool", pool);
        runner.setProperty(pool, AbstractRedisConnectionPoolService.CONNECTION_STRING, url());
        runner.setProperty(pool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        runner.enableControllerService(pool);
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "pool");
        runner.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cache");
        runner.setProperty(RedisScanReader.MIGRATION_ID, "load-" + System.nanoTime());
        runner.setProperty(RedisScanReader.PARTITION_COUNT, String.valueOf(partitions));
        runner.setProperty(RedisScanReader.SCAN_COUNT, "100");
        runner.setThreadCount(partitions);
        runner.run(partitions);
        List<String> keys = new ArrayList<>();
        for (MockFlowFile flowFile : runner.getFlowFilesForRelationship(RedisScanReader.REL_STRING)) {
            keys.add(flowFile.getAttribute("redis.key"));
        }
        return keys;
    }

    private static String url() {
        return "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379);
    }
}
