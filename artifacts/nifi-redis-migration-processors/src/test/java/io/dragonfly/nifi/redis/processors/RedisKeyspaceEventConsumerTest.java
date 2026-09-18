package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.FakeRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.FakeRedisPubSubHandle;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.List;

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
}
