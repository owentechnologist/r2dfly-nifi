package io.dragonfly.nifi.redis.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisScanReaderTest {

    @Test
    void exposesTheRelationshipsFromTheSpec() {
        TestRunner runner = TestRunners.newTestRunner(RedisScanReader.class);
        Set<String> names = runner.getProcessor().getRelationships().stream().map(r -> r.getName()).collect(Collectors.toSet());
        assertEquals(Set.of("string", "hash", "list", "set", "zset", "stream", "unknown", "failure"), names);
    }

    @Test
    void isNotValidWithoutRequiredProperties() {
        TestRunner runner = TestRunners.newTestRunner(RedisScanReader.class);
        runner.assertNotValid();
    }

    @Test
    void isNotValidWithAnUnsetControllerServiceReference() {
        TestRunner runner = TestRunners.newTestRunner(RedisScanReader.class);
        runner.setProperty(RedisScanReader.MIGRATION_ID, "migration-1");
        runner.setProperty(RedisScanReader.PARTITION_COUNT, "1");
        // REDIS_CONNECTION_POOL and CURSOR_STATE_CACHE are required but never set.
        runner.assertNotValid();
    }

    @Test
    void theThrottleBacksOffWhileTheSourceIsSlowAndRecoversAfter() {
        RedisScanReader.Throttle throttle = new RedisScanReader.Throttle(50, 0);
        List<Long> pauses = new ArrayList<>();
        for (long pageMs : new long[]{10, 80, 80, 80, 80, 80, 80, 80, 80, 10, 10, 10}) {
            pauses.add(throttle.pauseAfter(pageMs, () -> 0));
        }
        assertEquals(List.of(0L, 10L, 20L, 40L, 80L, 160L, 320L, 640L, 1000L, 500L, 250L, 125L), pauses);
    }

    @Test
    void theThrottleReadsOpsPerSecondAtMostOnceASecond() {
        RedisScanReader.Throttle throttle = new RedisScanReader.Throttle(0, 1000);
        int[] reads = {0};
        assertEquals(10L, throttle.pauseAfter(1, () -> { reads[0]++; return 5000; }));
        assertEquals(20L, throttle.pauseAfter(1, () -> { reads[0]++; return 0; }), "the busy reading still stands");
        assertEquals(1, reads[0]);
    }
}
