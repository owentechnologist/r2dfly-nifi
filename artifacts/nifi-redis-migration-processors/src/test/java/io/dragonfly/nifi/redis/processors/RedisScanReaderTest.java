package io.dragonfly.nifi.redis.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

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
}
