package io.dragonfly.nifi.redis.it;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.dragonfly.nifi.redis.processors.RedisScanReader;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.FakeDistributedMapCacheClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RedisScanReader against a real 3-master, 3-replica Redis Cluster and a standalone master with a
 * replica, all inside one container. Nodes announce 127.0.0.1 on fixed ports that the host maps
 * one to one, so the cluster bus works inside the container and the client reaches every node from
 * outside. SCAN load is counted on each node (INFO commandstats). Failover is off: on a loaded host
 * a master once missed the default 15 s node timeout mid-suite, its replica took over, and the
 * scan counts and keys moved with it (failover during a scan is ClusterTopologyIT's subject).
 * Requires Docker.
 */
@Testcontainers
class ClusterScanIT {

    private static final int[] CLUSTER_PORTS = {17001, 17002, 17003, 17004, 17005, 17006};
    private static final int MASTER_PORT = 17101;
    private static final int REPLICA_PORT = 17102;
    private static final int KEYS = 3000;

    private static final String START = String.join("; ",
            "for p in 17001 17002 17003 17004 17005 17006; do redis-server --port $p --cluster-enabled yes "
                    + "--cluster-config-file /tmp/n$p.conf --cluster-announce-ip 127.0.0.1 --repl-diskless-sync-delay 0 "
                    + "--cluster-node-timeout 60000 --cluster-replica-no-failover yes --daemonize yes --save ''; done",
            "redis-server --port 17101 --repl-diskless-sync-delay 0 --daemonize yes --save ''",
            "redis-server --port 17102 --replicaof 127.0.0.1 17101 --replica-announce-ip 127.0.0.1 --daemonize yes --save ''",
            "sleep 1",
            "yes yes | redis-cli --cluster create 127.0.0.1:17001 127.0.0.1:17002 127.0.0.1:17003 127.0.0.1:17004 "
                    + "127.0.0.1:17005 127.0.0.1:17006 --cluster-replicas 1",
            "until redis-cli -p 17001 cluster info | grep -q cluster_state:ok; do sleep 0.2; done",
            "until redis-cli -p 17102 info replication | grep -q master_link_status:up; do sleep 0.2; done",
            "sleep 2",
            "echo nodes-ready",
            "tail -f /dev/null");

    @Container
    static GenericContainer<?> nodes = new GenericContainer<>(DockerImageName.parse("redis:7.4"))
            .withCommand("sh", "-c", START)
            .withCreateContainerCmdModifier(cmd -> {
                List<PortBinding> bindings = new ArrayList<>();
                IntStream.concat(IntStream.of(CLUSTER_PORTS), IntStream.of(MASTER_PORT, REPLICA_PORT))
                        .forEach(p -> bindings.add(new PortBinding(Ports.Binding.bindPort(p), new ExposedPort(p))));
                cmd.getHostConfig().withPortBindings(bindings);
                cmd.withExposedPorts(bindings.stream().map(PortBinding::getExposedPort).toList());
            })
            .waitingFor(Wait.forLogMessage(".*nodes-ready.*", 1).withStartupTimeout(Duration.ofMinutes(2)));

    @BeforeAll
    static void seed() throws InterruptedException {
        RedisClusterClient client = RedisClusterClient.create("redis://127.0.0.1:17001");
        try (StatefulRedisClusterConnection<String, String> connection = client.connect()) {
            for (int i = 0; i < KEYS; i++) {
                connection.sync().set("k:" + i, "v");
            }
        } finally {
            client.shutdown();
        }
        try (Node master = connect(MASTER_PORT)) {
            for (int i = 0; i < KEYS; i++) {
                master.sync().set("k:" + i, "v");
            }
        }
        awaitReplicas();
    }

    /** Replicas apply writes asynchronously; the replica tests need every key there first. */
    private static void awaitReplicas() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        while (replicaPorts().stream().mapToLong(ClusterScanIT::dbsize).sum() < KEYS || dbsize(REPLICA_PORT) < KEYS) {
            assertTrue(System.currentTimeMillis() < deadline, () -> "replicas never caught up: " + replicationReport());
            Thread.sleep(100);
        }
    }

    @Test
    void eachMasterIsScannedOnceWhateverThePartitionCount() throws Exception {
        // Keys are counted distinct, since SCAN promises every key at least once rather than exactly
        // once; overlapping partitions show in the SCAN call counts.
        Map<Integer, Long> onePartition = scanCallsPerNode(() -> assertEquals(KEYS, new HashSet<>(scanCluster(1, "MASTER", null)).size()));
        Map<Integer, Long> twoPartitions = scanCallsPerNode(() -> assertEquals(KEYS, new HashSet<>(scanCluster(2, "MASTER", null)).size()));

        assertEquals(onePartition, twoPartitions, "per-node SCAN calls with 2 partitions vs 1");
        for (int replica : replicaPorts()) {
            assertEquals(0L, onePartition.get(replica), "replica " + replica + " is not scanned by default");
        }
    }

    @Test
    void aReplicaPreferringPoolScansAndReadsTheReplicas() throws Exception {
        Map<Integer, Long> scans = scanCallsPerNode(() -> assertEquals(KEYS, new HashSet<>(scanCluster(1, "REPLICA_PREFERRED", null)).size()));
        for (int master : masterPorts()) {
            assertEquals(0L, scans.get(master), "master " + master + " took no SCAN");
        }
        assertEquals(3, replicaPorts().stream().filter(p -> scans.get(p) > 0).count(), "every replica scanned its share: " + scans);
    }

    @Test
    void aClusterScanResumesFromItsCheckpointedNode() throws Exception {
        List<String> masters = sortedMasterIds();
        long laterMastersKeys = 0;
        for (String id : masters.subList(1, masters.size())) {
            laterMastersKeys += dbsize(portOf(id));
        }
        List<String> keys = scanCluster(1, "MASTER", masters.get(1) + "|" + masters.get(1) + "|0");
        assertEquals(laterMastersKeys, new HashSet<>(keys).size(), "only the checkpointed master and those after it were scanned");
    }

    @Test
    void aStandaloneReplicaPreferringPoolScansTheReplica() throws Exception {
        Map<Integer, Long> scans = scanCallsPerNode(() -> assertEquals(KEYS, new HashSet<>(scan("redis://127.0.0.1:" + MASTER_PORT, "STANDALONE",
                "REPLICA_PREFERRED", 1, null)).size()));
        assertEquals(0L, scans.get(MASTER_PORT), "the master took no SCAN");
        assertTrue(scans.get(REPLICA_PORT) > 0, "the replica did the scanning: " + scans);
    }

    private interface Body {
        void run() throws Exception;
    }

    private static Map<Integer, Long> scanCallsPerNode(Body body) throws Exception {
        List<Integer> ports = new ArrayList<>();
        IntStream.of(CLUSTER_PORTS).forEach(ports::add);
        ports.add(MASTER_PORT);
        ports.add(REPLICA_PORT);
        for (int port : ports) {
            try (Node c = connect(port)) {
                c.sync().configResetstat();
            }
        }
        body.run();
        Map<Integer, Long> calls = new TreeMap<>();
        for (int port : ports) {
            try (Node c = connect(port)) {
                long count = 0;
                for (String line : c.sync().info("commandstats").split("\r?\n")) {
                    if (line.startsWith("cmdstat_scan:")) {
                        count = Long.parseLong(line.replaceAll("^cmdstat_scan:calls=(\\d+),.*", "$1"));
                    }
                }
                calls.put(port, count);
            }
        }
        return calls;
    }

    private static List<String> scanCluster(int partitions, String readFrom, String checkpoint) throws Exception {
        return scan("redis://127.0.0.1:17001", "CLUSTER", readFrom, partitions, checkpoint);
    }

    private static List<String> scan(String url, String mode, String readFrom, int partitions, String checkpoint) throws Exception {
        TestRunner runner = TestRunners.newTestRunner(RedisScanReader.class);
        runner.setAllowSynchronousSessionCommits(true);
        StandardRedisConnectionPoolService pool = new StandardRedisConnectionPoolService();
        runner.addControllerService("pool", pool);
        runner.setProperty(pool, AbstractRedisConnectionPoolService.CONNECTION_STRING, url);
        runner.setProperty(pool, AbstractRedisConnectionPoolService.CONNECTION_MODE, mode);
        runner.setProperty(pool, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        runner.setProperty(pool, AbstractRedisConnectionPoolService.READ_FROM, readFrom);
        runner.enableControllerService(pool);
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        String migrationId = "cluster-" + System.nanoTime();
        if (checkpoint != null) {
            Serializer<String> serializer = (value, out) -> out.write(value.getBytes(StandardCharsets.UTF_8));
            cache.put("redis.migration.cursor.v2." + migrationId + ".0", checkpoint, serializer, serializer);
        }
        runner.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "pool");
        runner.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cache");
        runner.setProperty(RedisScanReader.MIGRATION_ID, migrationId);
        runner.setProperty(RedisScanReader.PARTITION_COUNT, String.valueOf(partitions));
        runner.setProperty(RedisScanReader.SCAN_COUNT, "100");
        runner.setThreadCount(partitions);
        runner.run(partitions);
        List<String> keys = new ArrayList<>();
        for (MockFlowFile flowFile : runner.getFlowFilesForRelationship(RedisScanReader.REL_STRING)) {
            keys.add(flowFile.getAttribute("redis.key"));
        }
        assertEquals(List.of(), runner.getFlowFilesForRelationship(RedisScanReader.REL_FAILURE));
        return keys;
    }

    /** Node id to port for the cluster's masters, from CLUSTER NODES. */
    private static Map<String, Integer> masters() {
        Map<String, Integer> masters = new TreeMap<>();
        try (Node c = connect(17001)) {
            for (String line : c.sync().clusterNodes().split("\n")) {
                String[] parts = line.split(" ");
                if (parts.length > 2 && parts[2].contains("master")) {
                    masters.put(parts[0], Integer.parseInt(parts[1].replaceAll(".*:(\\d+)@.*", "$1")));
                }
            }
        }
        return masters;
    }

    private static List<String> sortedMasterIds() {
        return new ArrayList<>(masters().keySet());
    }

    private static int portOf(String nodeId) {
        return masters().get(nodeId);
    }

    private static List<Integer> masterPorts() {
        return new ArrayList<>(masters().values());
    }

    private static List<Integer> replicaPorts() {
        List<Integer> replicas = new ArrayList<>();
        IntStream.of(CLUSTER_PORTS).filter(p -> !masterPorts().contains(p)).forEach(replicas::add);
        return replicas;
    }

    private static String replicationReport() {
        StringBuilder report = new StringBuilder();
        IntStream.concat(IntStream.of(CLUSTER_PORTS), IntStream.of(MASTER_PORT, REPLICA_PORT)).forEach(port -> {
            try (Node c = connect(port)) {
                String info = c.sync().info("replication");
                String role = info.replaceAll("(?s).*role:(\\w+).*", "$1");
                String link = info.contains("master_link_status:") ? info.replaceAll("(?s).*master_link_status:(\\w+).*", "$1") : "-";
                report.append(port).append('=').append(role).append('/').append(link).append('/').append(c.sync().dbsize()).append(' ');
            }
        });
        return report.toString();
    }

    private static long dbsize(int port) {
        try (Node c = connect(port)) {
            return c.sync().dbsize();
        }
    }

    /** One node's client and connection, closed together. */
    private record Node(RedisClient client, StatefulRedisConnection<String, String> connection) implements AutoCloseable {
        RedisCommands<String, String> sync() {
            return connection.sync();
        }

        @Override
        public void close() {
            connection.close();
            client.shutdown();
        }
    }

    private static Node connect(int port) {
        RedisClient client = RedisClient.create("redis://127.0.0.1:" + port);
        return new Node(client, client.connect());
    }
}
