package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.util.KeyFilter;
import io.dragonfly.nifi.redis.util.KeyMapping;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisKeyExistenceRouterTest {

    @Test
    void exposesTheFourRoutingRelationships() {
        TestRunner runner = TestRunners.newTestRunner(RedisKeyExistenceRouter.class);
        Set<String> names = runner.getProcessor().getRelationships().stream().map(r -> r.getName()).collect(Collectors.toSet());
        assertEquals(Set.of("exists", "missing", "filtered", "failure"), names);
    }

    @Test
    void isNotValidWithoutASourceConnectionPool() {
        TestRunner runner = TestRunners.newTestRunner(RedisKeyExistenceRouter.class);
        runner.assertNotValid();
    }

    @Test
    void stripsTheConfiguredPrefixFromTheTargetKey() {
        assertEquals(Optional.of("user:1"),
                scope("mig:user:1", KeyMapping.of(null, "mig", ":"), null, null));
    }

    @Test
    void returnsTheKeyUnchangedWhenNoPrefixIsConfigured() {
        assertEquals(Optional.of("user:1"),
                scope("user:1", KeyMapping.of(null, null, ":"), null, null));
    }

    @Test
    void filtersATargetKeyThatDoesNotCarryTheConfiguredPrefix() {
        assertEquals(Optional.empty(),
                scope("preexisting:1", KeyMapping.of(null, "mig", ":"), null, null));
    }

    @Test
    void filtersADeniedPrefixMeasuredAgainstTheUnprefixedKey() {
        assertEquals(Optional.empty(),
                scope("mig:cache:1", KeyMapping.of(null, "mig", ":"), "cache:", null));
    }

    @Test
    void filtersAKeyOutsideTheOnlyList() {
        assertEquals(Optional.empty(),
                scope("user:1", KeyMapping.of(null, null, ":"), null, "session:"));
        assertEquals(Optional.of("session:1"),
                scope("session:1", KeyMapping.of(null, null, ":"), null, "session:"));
    }

    @Test
    void appliesTheDenyListBeforeTheOnlyList() {
        assertEquals(Optional.empty(),
                scope("app:tmp:1", KeyMapping.of(null, null, ":"), "app:tmp:", "app:"));
    }

    @Test
    void mapsABinaryTargetKeyBackByItsBytes() {
        byte[] binary = {'m', 'i', 'g', ':', (byte) 0xff, 0x01};
        assertEquals(Optional.of(List.of((byte) 0xff, (byte) 0x01)), RedisKeyExistenceRouter.inScopeSourceKey(binary,
                KeyMapping.of(null, "mig", ":"), KeyFilter.parse(null, null, null)).map(RedisKeyExistenceRouterTest::list));
    }

    private static Optional<String> scope(String targetKey, KeyMapping mapping, String deny, String only) {
        return RedisKeyExistenceRouter.inScopeSourceKey(targetKey.getBytes(StandardCharsets.UTF_8), mapping, KeyFilter.parse(null, deny, only))
                .map(k -> new String(k, StandardCharsets.UTF_8));
    }

    private static List<Byte> list(byte[] bytes) {
        List<Byte> list = new java.util.ArrayList<>();
        for (byte b : bytes) {
            list.add(b);
        }
        return list;
    }
}
