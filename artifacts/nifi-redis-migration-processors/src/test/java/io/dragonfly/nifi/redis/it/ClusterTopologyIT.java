package io.dragonfly.nifi.redis.it;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.dragonfly.nifi.redis.processors.RedisReplicationReader;
import io.dragonfly.nifi.redis.processors.RedisScanReader;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.ClusterTopologySnapshot;
import io.dragonfly.nifi.redis.util.FakeDistributedMapCacheClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Topology changes under a live sync from a real Redis Cluster: a failover, a resharding, and a new
 * shard, each while writes keep arriving, with the target compared to the source after each. A
 * fresh 3-master, 3-replica cluster per test, plus a spare node for the new shard, in one
 * container on fixed ports mapped one to one (as in ClusterScanIT). Automatic failover is off so
 * that only the test moves the topology, and every change goes through a client rather than
 * redis-cli: podman's exec API drops connections under load. Requires Docker.
 */
class ClusterTopologyIT {

    private static final int[] PORTS = {17201, 17202, 17203, 17204, 17205, 17206, 17207};
    private static final String SEED = "redis://127.0.0.1:17201";
    private static final String NODE_FLAGS = "--cluster-enabled yes --cluster-announce-ip 127.0.0.1 --repl-diskless-sync-delay 0 "
            + "--cluster-node-timeout 60000 --cluster-replica-no-failover yes --daemonize yes --save ''";

    private static final GenericContainer<?> DRAGONFLY =
            new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest")).withExposedPorts(6379);

    static {
        DRAGONFLY.start();
    }

    private GenericContainer<?> nodes;
    private RedisClusterClient client;
    private StatefulRedisClusterConnection<String, String> source;
    private RedisClient targetClient;
    private StatefulRedisConnection<String, String> target;

    @BeforeEach
    void startCluster() {
        String start = String.join("; ",
                "for p in 17201 17202 17203 17204 17205 17206 17207; do redis-server --port $p --cluster-config-file /tmp/n$p.conf " + NODE_FLAGS + "; done",
                "sleep 1",
                "yes yes | redis-cli --cluster create 127.0.0.1:17201 127.0.0.1:17202 127.0.0.1:17203 127.0.0.1:17204 "
                        + "127.0.0.1:17205 127.0.0.1:17206 --cluster-replicas 1",
                "until redis-cli -p 17201 cluster info | grep -q cluster_state:ok; do sleep 0.2; done",
                "sleep 2",
                "echo nodes-ready",
                "tail -f /dev/null");
        nodes = new GenericContainer<>(DockerImageName.parse("redis:7.4"))
                .withCommand("sh", "-c", start)
                .withCreateContainerCmdModifier(cmd -> {
                    List<PortBinding> bindings = new ArrayList<>();
                    IntStream.of(PORTS).forEach(p -> bindings.add(new PortBinding(Ports.Binding.bindPort(p), new ExposedPort(p))));
                    cmd.getHostConfig().withPortBindings(bindings);
                    cmd.withExposedPorts(bindings.stream().map(PortBinding::getExposedPort).toList());
                })
                .waitingFor(Wait.forLogMessage(".*nodes-ready.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
        nodes.start();
        client = RedisClusterClient.create(SEED);
        client.setOptions(ClusterClientOptions.builder()
                .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder().enableAllAdaptiveRefreshTriggers().build()).build());
        source = client.connect();
        targetClient = RedisClient.create(ReplicationPipeline.url(DRAGONFLY));
        target = targetClient.connect();
        target.sync().flushall();
    }

    @AfterEach
    void stopCluster() {
        source.close();
        client.shutdown();
        target.close();
        targetClient.shutdown();
        nodes.stop();
    }

    @Test
    void aLiveSyncSurvivesAFailoverAReshardAndANewShard() throws Exception {
        RedisAdvancedClusterCommands<String, String> s = source.sync();
        for (int i = 0; i < 2000; i++) {
            s.set("k:" + i, "v" + i);
            s.hset("h:" + i, Map.of("f", String.valueOf(i)));
        }
        long startedAt = System.currentTimeMillis() / 1000;
        ReplicationPipeline pipeline = new ReplicationPipeline(SEED, "CLUSTER", DRAGONFLY, reader());
        pipeline.pumpUntilQuiet();
        ReplicationPipeline.assertTargetMatchesSource(s, target.sync(), false);
        assertEquals(3L, pipeline.counter("Replication Full Resyncs"), "one snapshot per shard");
        assertTrue(pipeline.counter("Replication Streams Connected At (epoch seconds)") >= startedAt, "every shard's stream connected");

        String failedOver = masters().keySet().iterator().next();
        underWrites(pipeline, 1, () -> failover(failedOver));
        ReplicationPipeline.assertTargetMatchesSource(s, target.sync(), false);
        assertEquals(1L, pipeline.counter("Replication Failover Resumes"), "the promoted replica continued the old master's stream");
        assertEquals(3L, pipeline.counter("Replication Full Resyncs"), "no shard was copied again after the failover");

        List<String> ids = new ArrayList<>(masters().keySet());
        underWrites(pipeline, 2, () -> reshard(ids.get(0), ids.get(1), 500));
        ReplicationPipeline.assertTargetMatchesSource(s, target.sync(), false);
        assertEquals(3L, pipeline.counter("Replication Full Resyncs"), "a resharding copies only the keys it moves");

        underWrites(pipeline, 3, () -> {
            String added = addShard();
            reshard(ids.get(1), added, 500);
        });
        long deadline = System.currentTimeMillis() + 60_000;
        while (pipeline.counter("Replication New Shards") < 4) {
            assertTrue(System.currentTimeMillis() < deadline, "the new shard never got a stream");
            pipeline.step();
        }
        pipeline.pumpUntilQuiet();
        ReplicationPipeline.assertTargetMatchesSource(s, target.sync(), false);
        assertEquals(4L, pipeline.counter("Replication Full Resyncs"), "only the new shard took a full resync");

        // A restart resumes every shard from its own saved position.
        Map<String, String> saved = pipeline.reader.getStateManager().getState(Scope.CLUSTER).toMap();
        assertEquals(4, saved.keySet().stream().filter(k -> k.startsWith("stream.")).count(), "one saved position per master: " + saved);
        pipeline.reader.run(1, true, false);
        s.set("after:restart", "1");
        TestRunner restarted = reader();
        restarted.getStateManager().setState(saved, Scope.CLUSTER);
        ReplicationPipeline resumed = new ReplicationPipeline(SEED, "CLUSTER", DRAGONFLY, restarted);
        resumed.pumpUntilQuiet();
        ReplicationPipeline.assertTargetMatchesSource(s, target.sync(), false);
        assertEquals(0L, resumed.counter("Replication Full Resyncs"));
        assertEquals(4L, resumed.counter("Replication Partial Resyncs"));
        resumed.reader.run(1, true, false);
    }

    /**
     * The scan phase, picked up mid-scan from a checkpoint in the second shard. The first shard is
     * done and must not be scanned again whatever moves; what the change touched is read again.
     */
    @Test
    void aFailoverMidScanRestartsOnlyThatShard() throws Exception {
        MidScan scan = new MidScan();
        failover(scan.current);
        List<String> emitted = scan.run();
        assertEquals(0L, scan.scanCalls.get(scan.done), "the finished shard was not scanned again");
        assertTrue(emitted.containsAll(keysIn(scan.currentSlots)), "every key of the failed-over shard, scanned again on its new master");
        assertTrue(emitted.containsAll(keysIn(scan.laterSlots)));
        assertEquals(1L, scan.runner.getCounterValue("Scan Shards Restarted After Failover"));
    }

    @Test
    void slotsMovedOffTheShardMidScanAreReadOnTheirNewOwner() throws Exception {
        MidScan scan = new MidScan();
        List<String> moved = moveSlots(scan.current, scan.done, 40);
        List<String> emitted = scan.run();
        assertTrue(emitted.containsAll(moved), "the keys moved to the finished shard were read there");
        assertEquals(0L, scan.scanCalls.get(scan.done), "by slot, without a SCAN of the finished shard");
        assertEquals(40L, scan.runner.getCounterValue("Scan Slots Rescanned"));
        assertTrue(emitted.stream().noneMatch(k -> scan.doneKeys.contains(k) && !moved.contains(k)), "nothing else of the finished shard");
    }

    @Test
    void slotsMovedToANewShardMidScanAreReadThere() throws Exception {
        MidScan scan = new MidScan();
        String added = addShard();
        List<String> moved = moveSlots(scan.current, added, 40);
        List<String> emitted = scan.run();
        assertTrue(emitted.containsAll(moved), "the keys moved to the new shard were read there");
        assertEquals(0L, scan.scanCalls.get(scan.done));
        assertEquals(40L, scan.runner.getCounterValue("Scan Slots Rescanned"));
    }

    /** Seeds keys, records the baseline topology, and leaves a checkpoint partway through the second shard's SCAN. */
    private final class MidScan {
        final String done;
        final String current;
        final String later;
        final List<String> doneKeys;
        final List<Integer> currentSlots;
        final List<Integer> laterSlots;
        final String migrationId = "topology-" + System.nanoTime();
        final FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        final Map<String, Long> scanCalls = new TreeMap<>();
        TestRunner runner;

        MidScan() throws Exception {
            RedisAdvancedClusterCommands<String, String> s = source.sync();
            for (int i = 0; i < 3000; i++) {
                s.set("k:" + i, "v");
            }
            List<String> ids = new ArrayList<>(masters().keySet());
            done = ids.get(0);
            current = ids.get(1);
            later = ids.get(2);
            doneKeys = keysIn(slotsOf(done));
            currentSlots = slotsOf(current);
            laterSlots = slotsOf(later);
            List<ClusterTopologySnapshot.MasterNode> baseline = new ArrayList<>();
            masters().forEach((id, port) -> baseline.add(new ClusterTopologySnapshot.MasterNode(id, "127.0.0.1", port, java.util.Set.copyOf(slotsOf(id)))));
            put("redis.migration.topology.v2." + migrationId, ClusterTopologySnapshot.of(System.currentTimeMillis(), baseline).encode());
            String cursor = node(masters().get(current), c -> c.scan(io.lettuce.core.ScanArgs.Builder.limit(20))).getCursor();
            put("redis.migration.cursor.v2." + migrationId + ".0", current + "|" + current + "|" + cursor);
        }

        List<String> run() throws Exception {
            for (int port : PORTS) {
                node(port, c -> c.configResetstat());
            }
            runner = TestRunners.newTestRunner(RedisScanReader.class);
            runner.setAllowSynchronousSessionCommits(true);
            ReplicationPipeline.addPool(runner, "pool", new StandardRedisConnectionPoolService(), SEED, "CLUSTER");
            runner.addControllerService("cache", cache);
            runner.enableControllerService(cache);
            runner.setProperty(RedisScanReader.REDIS_CONNECTION_POOL, "pool");
            runner.setProperty(RedisScanReader.CURSOR_STATE_CACHE, "cache");
            runner.setProperty(RedisScanReader.MIGRATION_ID, migrationId);
            runner.setProperty(RedisScanReader.SCAN_COUNT, "100");
            runner.run();
            assertEquals(List.of(), runner.getFlowFilesForRelationship(RedisScanReader.REL_FAILURE));
            for (String id : List.of(done, current, later)) {
                Integer port = allMasters().get(id);
                scanCalls.put(id, port == null ? 0L : scanCalls(port));
            }
            return runner.getFlowFilesForRelationship(RedisScanReader.REL_STRING).stream().map(f -> f.getAttribute("redis.key")).toList();
        }

        private void put(String key, String value) throws Exception {
            org.apache.nifi.distributed.cache.client.Serializer<String> serializer =
                    (v, out) -> out.write(v.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            cache.put(key, value, serializer, serializer);
        }
    }

    private static long scanCalls(int port) {
        for (String line : node(port, c -> c.info("commandstats")).split("\r?\n")) {
            if (line.startsWith("cmdstat_scan:")) {
                return Long.parseLong(line.replaceAll("^cmdstat_scan:calls=(\\d+),.*", "$1"));
            }
        }
        return 0;
    }

    /** Every key in these slots, read on whichever master owns each slot now. */
    private List<String> keysIn(List<Integer> slots) {
        Map<Integer, Integer> ownerPort = new TreeMap<>();
        masters().forEach((id, port) -> slotsOf(id).forEach(slot -> ownerPort.put(slot, port)));
        List<String> keys = new ArrayList<>();
        for (int slot : slots) {
            keys.addAll(node(ownerPort.get(slot), c -> c.clusterGetKeysInSlot(slot, 10_000)));
        }
        return keys;
    }

    /** Moves the first {@code slots} slots that hold keys; returns the keys moved. */
    private List<String> moveSlots(String from, String to, int slots) throws Exception {
        int port = allMasters().get(from);
        List<Integer> withKeys = slotsOf(from).stream().filter(slot -> node(port, c -> c.clusterCountKeysInSlot(slot)) > 0).limit(slots).toList();
        List<String> moved = new ArrayList<>();
        for (int slot : withKeys) {
            moved.addAll(node(port, c -> c.clusterGetKeysInSlot(slot, 10_000)));
        }
        migrate(from, to, withKeys);
        return moved;
    }

    private interface Step {
        void run() throws Exception;
    }

    /** Runs the topology change while a writer keeps changing keys on every shard, then lets the flow drain. */
    private void underWrites(ReplicationPipeline pipeline, long seed, Step change) throws Exception {
        AtomicBoolean stop = new AtomicBoolean();
        Thread writer = new Thread(() -> {
            Random random = new Random(seed);
            RedisAdvancedClusterCommands<String, String> w = source.sync();
            while (!stop.get()) {
                int n = random.nextInt(2000);
                try {
                    switch (random.nextInt(4)) {
                        case 0 -> w.set("k:" + n, "w" + random.nextInt());
                        case 1 -> w.incr("counter:" + (n % 50));
                        case 2 -> w.del("k:" + n);
                        default -> w.hset("h:" + n, "g", String.valueOf(n));
                    }
                } catch (RuntimeException e) {
                    // a write refused mid-failover; the source simply does not have it
                }
            }
        });
        writer.start();
        try {
            pipeline.pumpFor(1000);
            change.run();
            pipeline.pumpFor(7000);
        } finally {
            stop.set(true);
            writer.join();
        }
        pipeline.pumpUntilQuiet();
    }

    private TestRunner reader() throws Exception {
        TestRunner reader = TestRunners.newTestRunner(RedisReplicationReader.class);
        reader.setAllowSynchronousSessionCommits(true);
        ReplicationPipeline.addPool(reader, "source-pool", new StandardRedisConnectionPoolService(), SEED, "CLUSTER");
        reader.setProperty(RedisReplicationReader.REDIS_CONNECTION_POOL, "source-pool");
        reader.setProperty(RedisReplicationReader.SCAN_COUNT, "100");
        return reader;
    }

    /** Promotes the master's replica, as an operator's CLUSTER FAILOVER does. */
    private void failover(String masterId) throws Exception {
        int replica = replicaOf(masterId);
        node(replica, c -> c.clusterFailover(false));
        long deadline = System.currentTimeMillis() + 30_000;
        while (!masters().containsValue(replica)) {
            assertTrue(System.currentTimeMillis() < deadline, "the replica on " + replica + " was never promoted");
            Thread.sleep(200);
        }
    }

    /** Moves the first {@code slots} of one master's slots to another, key by key, as redis-cli --cluster reshard does. */
    private void reshard(String from, String to, int slots) throws Exception {
        migrate(from, to, slotsOf(from).subList(0, slots));
    }

    private void migrate(String from, String to, List<Integer> moving) throws Exception {
        Map<String, Integer> masters = allMasters();
        int fromPort = masters.get(from);
        int toPort = masters.get(to);
        for (int slot : moving) {
            node(toPort, c -> c.clusterSetSlotImporting(slot, from));
            node(fromPort, c -> c.clusterSetSlotMigrating(slot, to));
            List<String> keys;
            while (!(keys = node(fromPort, c -> c.clusterGetKeysInSlot(slot, 100))).isEmpty()) {
                List<String> batch = keys;
                node(fromPort, c -> c.migrate("127.0.0.1", toPort, 0, 5000, io.lettuce.core.MigrateArgs.Builder.keys(batch)));
            }
            for (int port : masters.values()) {
                node(port, c -> c.clusterSetSlotNode(slot, to));
            }
        }
        awaitClusterOk();
    }

    private List<Integer> slotsOf(String masterId) {
        for (String line : node(17201, c -> c.clusterNodes()).split("\n")) {
            String[] parts = line.trim().split(" ");
            if (parts[0].equals(masterId)) {
                List<Integer> slots = new ArrayList<>();
                for (int i = 8; i < parts.length; i++) {
                    String[] range = parts[i].split("-");
                    int low = Integer.parseInt(range[0]);
                    int high = range.length > 1 ? Integer.parseInt(range[1]) : low;
                    IntStream.rangeClosed(low, high).forEach(slots::add);
                }
                return slots;
            }
        }
        throw new AssertionError("no master " + masterId);
    }

    /** Joins the spare node, started with the others but left out of the cluster, as a master without slots; returns its node id. */
    private String addShard() throws Exception {
        node(17201, c -> c.clusterMeet("127.0.0.1", 17207));
        long deadline = System.currentTimeMillis() + 30_000;
        while (!IntStream.of(PORTS).allMatch(p -> node(p, c -> c.clusterInfo()).contains("cluster_known_nodes:7"))
                || !node(17207, c -> c.clusterInfo()).contains("cluster_state:ok")) {
            assertTrue(System.currentTimeMillis() < deadline, "the new node never joined");
            Thread.sleep(200);
        }
        return node(17207, c -> c.clusterMyId());
    }

    private void awaitClusterOk() throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        // Settled: every slot assigned, no slot mid-migration, and every node agrees on the config.
        while (!node(17201, c -> c.clusterInfo()).contains("cluster_slots_ok:16384")
                || node(17201, c -> c.clusterNodes()).contains("[")
                || IntStream.of(PORTS).filter(p -> !node(p, c -> c.clusterInfo()).contains("cluster_known_nodes:1\r")).mapToObj(p -> node(p, c -> c.clusterInfo()).replaceAll("(?s).*cluster_current_epoch:(\\d+).*", "$1")).distinct().count() > 1) {
            assertTrue(System.currentTimeMillis() < deadline, "the cluster never settled");
            Thread.sleep(200);
        }
    }

    /** Node id to port for every master, with slots or not. */
    private Map<String, Integer> allMasters() {
        Map<String, Integer> masters = new TreeMap<>();
        for (String line : node(17201, c -> c.clusterNodes()).split("\n")) {
            String[] parts = line.trim().split(" ");
            if (parts.length > 7 && List.of(parts[2].split(",")).contains("master")) {
                masters.put(parts[0], Integer.parseInt(parts[1].replaceAll(".*:(\\d+)@.*", "$1")));
            }
        }
        return masters;
    }

    /** Node id to port for the slot-owning masters, from CLUSTER NODES. */
    private Map<String, Integer> masters() {
        Map<String, Integer> masters = new TreeMap<>();
        String nodes = node(17201, c -> c.clusterNodes());
        for (String line : nodes.split("\n")) {
            String[] parts = line.trim().split(" ");
            List<String> flags = List.of(parts.length > 2 ? parts[2].split(",") : new String[0]);
            if (parts.length > 8 && flags.contains("master") && !flags.contains("fail") && !flags.contains("fail?")) {
                masters.put(parts[0], Integer.parseInt(parts[1].replaceAll(".*:(\\d+)@.*", "$1")));
            }
        }
        assertTrue(!masters.isEmpty(), "no slot-owning master in: " + nodes);
        return masters;
    }

    private int replicaOf(String masterId) {
        for (String line : node(17201, c -> c.clusterNodes()).split("\n")) {
            String[] parts = line.trim().split(" ");
            if (parts.length > 3 && parts[3].equals(masterId)) {
                return Integer.parseInt(parts[1].replaceAll(".*:(\\d+)@.*", "$1"));
            }
        }
        throw new AssertionError("no replica of " + masterId);
    }

    /** One command on one node. Read over a client, not redis-cli: podman's exec hands back no output. */
    private static <T> T node(int port, java.util.function.Function<io.lettuce.core.api.sync.RedisCommands<String, String>, T> command) {
        RedisClient c = RedisClient.create("redis://127.0.0.1:" + port);
        try (StatefulRedisConnection<String, String> connection = c.connect()) {
            return command.apply(connection.sync());
        } finally {
            c.shutdown();
        }
    }


}
