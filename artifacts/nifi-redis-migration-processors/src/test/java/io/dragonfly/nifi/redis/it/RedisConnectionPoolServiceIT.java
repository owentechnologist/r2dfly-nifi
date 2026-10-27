package io.dragonfly.nifi.redis.it;

import io.dragonfly.nifi.redis.processors.RedisScanReader;
import io.dragonfly.nifi.redis.services.AbstractRedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardDragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.StandardRedisConnectionPoolService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the controller services actually connect to real Redis and Dragonfly containers and
 * can round-trip a value, per plan Phase 1. Requires Docker; run via {@code mvn verify}.
 */
@Testcontainers
class RedisConnectionPoolServiceIT {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7"))
            .withExposedPorts(6379);

    @Container
    static GenericContainer<?> dragonfly = new GenericContainer<>(DockerImageName.parse("docker.dragonflydb.io/dragonflydb/dragonfly:latest"))
            .withExposedPorts(6379);

    @Test
    void connectsToStandaloneRedisAndRoundTripsAValue() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(RedisScanReader.class);
        StandardRedisConnectionPoolService service = new StandardRedisConnectionPoolService();
        runner.addControllerService("redis-pool", service);
        runner.setProperty(service, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        runner.setProperty(service, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        runner.enableControllerService(service);

        String result = service.withConnection(cmds -> {
            try {
                cmds.set("it-key".getBytes(StandardCharsets.UTF_8), "it-value".getBytes(StandardCharsets.UTF_8)).get();
                byte[] value = cmds.get("it-key".getBytes(StandardCharsets.UTF_8)).get();
                return new String(value, StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals("it-value", result);
    }

    @Test
    void usesTheDatabaseNamedInTheConnectionString() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(RedisScanReader.class);
        StandardRedisConnectionPoolService service = new StandardRedisConnectionPoolService();
        runner.addControllerService("redis-pool", service);
        runner.setProperty(service, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379) + "/3");
        runner.setProperty(service, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        runner.enableControllerService(service);
        service.withConnection(cmds -> {
            try {
                return cmds.set("db-key".getBytes(StandardCharsets.UTF_8), "in-db-3".getBytes(StandardCharsets.UTF_8)).get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        try (io.lettuce.core.RedisClient client = io.lettuce.core.RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
             var connection = client.connect()) {
            assertEquals(0L, connection.sync().exists("db-key"), "not written to database 0");
            connection.sync().select(3);
            assertEquals("in-db-3", connection.sync().get("db-key"));
        }
    }

    @Test
    void connectsToDragonflyAndRoundTripsAValue() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(RedisScanReader.class);
        StandardDragonflyConnectionPoolService service = new StandardDragonflyConnectionPoolService();
        runner.addControllerService("dragonfly-pool", service);
        runner.setProperty(service, AbstractRedisConnectionPoolService.CONNECTION_STRING,
                "redis://" + dragonfly.getHost() + ":" + dragonfly.getMappedPort(6379));
        runner.setProperty(service, AbstractRedisConnectionPoolService.REQUIRE_TLS, "false");
        runner.enableControllerService(service);

        String result = service.withConnection(cmds -> {
            try {
                cmds.set("it-key".getBytes(StandardCharsets.UTF_8), "it-value".getBytes(StandardCharsets.UTF_8)).get();
                byte[] value = cmds.get("it-key".getBytes(StandardCharsets.UTF_8)).get();
                return new String(value, StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals("it-value", result);
    }
}
