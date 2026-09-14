package io.dragonfly.nifi.redis.services;

import io.dragonfly.nifi.redis.processors.RedisScanReader;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StandardRedisConnectionPoolServiceTest {

    private TestRunner runner;
    private StandardRedisConnectionPoolService service;

    @BeforeEach
    void setUp() throws Exception {
        // Hosted under a real processor since NiFi controller service validation runs in the
        // context of a TestRunner, not standalone.
        runner = TestRunners.newTestRunner(RedisScanReader.class);
        service = new StandardRedisConnectionPoolService();
        runner.addControllerService("redis-pool", service);
        runner.setProperty(service, AbstractRedisConnectionPoolService.CONNECTION_STRING, "redis://localhost:6379");
    }

    @Test
    void requiresSslContextServiceWhenTlsIsRequired() {
        runner.setProperty(service, AbstractRedisConnectionPoolService.REQUIRE_TLS, "true");
        runner.assertNotValid(service);
    }

    @Test
    void isValidWithoutSslContextServiceWhenTlsIsNotRequired() {
        runner.setProperty(service, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        runner.assertValid(service);
    }
}
