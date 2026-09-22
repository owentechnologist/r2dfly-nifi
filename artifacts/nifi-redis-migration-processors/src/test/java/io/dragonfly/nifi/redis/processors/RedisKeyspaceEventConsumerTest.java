package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.FakeRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.FakeRedisPubSubHandle;
import io.dragonfly.nifi.redis.util.ClusterTopologySnapshot;
import io.dragonfly.nifi.redis.util.FakeDistributedMapCacheClient;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisKeyspaceEventConsumerTest {

    @Test
    void parsesAKeyeventChannel() {
        RedisKeyspaceEventConsumer.ParsedEvent event = RedisKeyspaceEventConsumer.parse("__keyevent@0__:set", "user:session:abc123");

        assertEquals("user:session:abc123", event.key());
        assertEquals("set", event.eventType());
        assertEquals(0, event.database());
    }

    @Test
    void parsesAKeyspaceChannel() {
        RedisKeyspaceEventConsumer.ParsedEvent event = RedisKeyspaceEventConsumer.parse("__keyspace@3__:user:session:abc123", "del");

        assertEquals("user:session:abc123", event.key());
        assertEquals("del", event.eventType());
        assertEquals(3, event.database());
    }

    @Test
    void returnsNullForAnUnrecognizedChannel() {
        assertNull(RedisKeyspaceEventConsumer.parse("some.other.channel", "payload"));
    }

    @Test
    void countsDroppedEventsWithoutLoggingEachOne() throws Exception {
        TestRunner runner = newRunner();
        runner.setProperty(RedisKeyspaceEventConsumer.MAX_QUEUE_DEPTH, "1");
        FakeRedisPubSubHandle handle = start(runner);

        handle.deliver("__keyevent@0__:set", "key:a");
        handle.deliver("__keyevent@0__:set", "key:b");
        handle.deliver("__keyevent@0__:set", "key:c");
        runner.run(1, false, false);

        assertEquals(2L, runner.getCounterValue("Keyspace Events Dropped (Queue Full)"));
        assertEquals(List.of(), warnings(runner));
    }

    @Test
    void reportsADisconnectAndTheFollowingReconnect() throws Exception {
        TestRunner runner = newRunner();
        FakeRedisPubSubHandle handle = start(runner);

        handle.simulateDisconnect();
        Thread.sleep(5);
        handle.simulateReconnect();
        runner.run(1, false, false);

        assertEquals(1L, runner.getCounterValue("Keyspace Pub/Sub Disconnects"));
        assertTrue(runner.getCounterValue("Keyspace Pub/Sub Downtime (ms)") > 0L);
        assertEquals(1, matching(runner, "connection lost").size());
        assertEquals(1, matching(runner, "connection restored").size());
        assertEquals(2, warnings(runner).size());
    }

    @Test
    void throttlesRepeatedWarningsWhileStillCountingEveryDisconnect() throws Exception {
        TestRunner runner = newRunner();
        runner.setProperty(RedisKeyspaceEventConsumer.RECONNECT_BACKOFF_MS, "60000");
        FakeRedisPubSubHandle handle = start(runner);

        handle.simulateDisconnect();
        handle.simulateReconnect();
        handle.simulateDisconnect();
        handle.simulateReconnect();
        runner.run(1, false, false);

        assertEquals(2L, runner.getCounterValue("Keyspace Pub/Sub Disconnects"));
        assertEquals(1, matching(runner, "connection lost").size());
        assertEquals(1, matching(runner, "connection restored").size());
    }

    @Test
    void ignoresADuplicateReconnectCallback() throws Exception {
        TestRunner runner = newRunner();
        FakeRedisPubSubHandle handle = start(runner);

        long start = System.currentTimeMillis();
        handle.simulateDisconnect();
        Thread.sleep(20);
        handle.simulateReconnect();
        long elapsedMs = System.currentTimeMillis() - start;
        handle.simulateReconnect();
        runner.run(1, false, false);

        long downtimeMs = runner.getCounterValue("Keyspace Pub/Sub Downtime (ms)");
        assertTrue(downtimeMs > 0L);
        // A doubled reconnect would bank the outage twice, exceeding the wall clock that contained it.
        assertTrue(downtimeMs <= elapsedMs, "downtime " + downtimeMs + " ms exceeds the " + elapsedMs + " ms it was measured within");
        assertEquals(1, matching(runner, "connection restored").size());
    }

    @Test
    void reportsCriticalDriftWhenANewMasterJoins() throws Exception {
        TestRunner runner = newRunner();
        runner.setProperty(RedisKeyspaceEventConsumer.TOPOLOGY_CHECK_INTERVAL_MS, "1");
        FakeRedisConnectionPoolService pool = pool(runner);
        pool.setTopology(topology("a"));

        runner.run(1, false);
        pool.setTopology(topology("a", "b"));
        Thread.sleep(5);
        runner.run(1, false, false);

        assertEquals(1L, counterValue(runner, "Cluster Topology Drift Detected (Critical)"));
        List<MockFlowFile> drifted = runner.getFlowFilesForRelationship(RedisKeyspaceEventConsumer.REL_TOPOLOGY_DRIFT);
        assertEquals(1, drifted.size());
        assertEquals("b", drifted.get(0).getAttribute("redis.topology.drift.newly_joined_master_ids"));
        assertEquals("CRITICAL", drifted.get(0).getAttribute("redis.topology.drift.severity"));
        assertEquals(1, matchingErrors(runner, "Master(s) b joined").size());
    }

    @Test
    void reportsAdvisoryDriftWithoutAFlowFileWhenAMasterDeparts() throws Exception {
        TestRunner runner = newRunner();
        runner.setProperty(RedisKeyspaceEventConsumer.TOPOLOGY_CHECK_INTERVAL_MS, "1");
        FakeRedisConnectionPoolService pool = pool(runner);
        pool.setTopology(topology("a", "b"));

        runner.run(1, false);
        pool.setTopology(topology("a"));
        Thread.sleep(5);
        runner.run(1, false, false);

        assertEquals(1L, counterValue(runner, "Cluster Topology Drift Detected (Advisory)"));
        assertEquals(0L, counterValue(runner, "Cluster Topology Drift Detected (Critical)"));
        assertEquals(List.of(), runner.getFlowFilesForRelationship(RedisKeyspaceEventConsumer.REL_TOPOLOGY_DRIFT));
        assertEquals(List.of(), errors(runner));
    }

    @Test
    void restoresTheTopologyBaselineFromTheCacheAcrossARestart() throws Exception {
        TestRunner runner = newRunner();
        FakeDistributedMapCacheClient cache = new FakeDistributedMapCacheClient();
        runner.addControllerService("topology-cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(RedisKeyspaceEventConsumer.TOPOLOGY_STATE_CACHE, "topology-cache");
        runner.setProperty(RedisKeyspaceEventConsumer.TOPOLOGY_CHECK_INTERVAL_MS, "1");
        FakeRedisConnectionPoolService pool = pool(runner);
        pool.setTopology(topology("a"));

        runner.run(1, true);
        pool.setTopology(topology("a", "b"));
        Thread.sleep(5);
        runner.run(1, true);

        assertEquals(1L, counterValue(runner, "Cluster Topology Drift Detected (Critical)"));
    }

    @Test
    void reBaselinesAcrossARestartWhenNoCacheIsConfigured() throws Exception {
        TestRunner runner = newRunner();
        runner.setProperty(RedisKeyspaceEventConsumer.TOPOLOGY_CHECK_INTERVAL_MS, "1");
        FakeRedisConnectionPoolService pool = pool(runner);
        pool.setTopology(topology("a"));

        runner.run(1, true);
        pool.setTopology(topology("a", "b"));
        Thread.sleep(5);
        runner.run(1, true);

        assertEquals(0L, counterValue(runner, "Cluster Topology Drift Detected (Critical)"));
    }

    private static TestRunner newRunner() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(RedisKeyspaceEventConsumer.class);
        FakeRedisConnectionPoolService pool = new FakeRedisConnectionPoolService();
        runner.addControllerService("redis-pool", pool);
        runner.enableControllerService(pool);
        runner.setProperty(RedisKeyspaceEventConsumer.REDIS_CONNECTION_POOL, "redis-pool");
        return runner;
    }

    private static FakeRedisPubSubHandle start(TestRunner runner) {
        runner.run(1, false);
        return runner.getControllerService("redis-pool", FakeRedisConnectionPoolService.class).handle();
    }

    private static List<String> warnings(TestRunner runner) {
        return runner.getLogger().getWarnMessages().stream().map(LogMessage::getMsg).toList();
    }

    private static List<String> matching(TestRunner runner, String fragment) {
        return warnings(runner).stream().filter(msg -> msg.contains(fragment)).toList();
    }

    private static List<String> errors(TestRunner runner) {
        return runner.getLogger().getErrorMessages().stream().map(LogMessage::getMsg).toList();
    }

    private static List<String> matchingErrors(TestRunner runner, String fragment) {
        return errors(runner).stream().filter(msg -> msg.contains(fragment)).toList();
    }

    private static FakeRedisConnectionPoolService pool(TestRunner runner) {
        return runner.getControllerService("redis-pool", FakeRedisConnectionPoolService.class);
    }

    /** A counter no session ever adjusted is absent rather than zero. */
    private static long counterValue(TestRunner runner, String name) {
        Long value = runner.getCounterValue(name);
        return value == null ? 0L : value;
    }

    /** One master per id, each owning an equal, disjoint share of the 16384 slots. */
    private static ClusterTopologySnapshot topology(String... nodeIds) {
        List<ClusterTopologySnapshot.MasterNode> masters = new ArrayList<>();
        int slotsPerMaster = 16384 / nodeIds.length;
        for (int i = 0; i < nodeIds.length; i++) {
            int firstSlot = i * slotsPerMaster;
            int lastSlot = (i == nodeIds.length - 1) ? 16383 : firstSlot + slotsPerMaster - 1;
            Set<Integer> slots = IntStream.rangeClosed(firstSlot, lastSlot).boxed().collect(Collectors.toSet());
            masters.add(new ClusterTopologySnapshot.MasterNode(nodeIds[i], "10.0.0." + (i + 1), 6379, slots));
        }
        return ClusterTopologySnapshot.of(System.currentTimeMillis(), masters);
    }
}
