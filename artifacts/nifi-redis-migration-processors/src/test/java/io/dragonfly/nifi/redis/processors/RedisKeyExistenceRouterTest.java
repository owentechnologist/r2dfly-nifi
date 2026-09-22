package io.dragonfly.nifi.redis.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

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
                RedisKeyExistenceRouter.inScopeSourceKey("mig:user:1", "mig", ":", List.of(), List.of()));
    }

    @Test
    void returnsTheKeyUnchangedWhenNoPrefixIsConfigured() {
        assertEquals(Optional.of("user:1"),
                RedisKeyExistenceRouter.inScopeSourceKey("user:1", null, ":", List.of(), List.of()));
    }

    @Test
    void filtersATargetKeyThatDoesNotCarryTheConfiguredPrefix() {
        assertEquals(Optional.empty(),
                RedisKeyExistenceRouter.inScopeSourceKey("preexisting:1", "mig", ":", List.of(), List.of()));
    }

    @Test
    void filtersADeniedPrefixMeasuredAgainstTheUnprefixedKey() {
        assertEquals(Optional.empty(),
                RedisKeyExistenceRouter.inScopeSourceKey("mig:cache:1", "mig", ":", List.of("cache:"), List.of()));
    }

    @Test
    void filtersAKeyOutsideTheOnlyList() {
        assertEquals(Optional.empty(),
                RedisKeyExistenceRouter.inScopeSourceKey("user:1", null, ":", List.of(), List.of("session:")));
        assertEquals(Optional.of("session:1"),
                RedisKeyExistenceRouter.inScopeSourceKey("session:1", null, ":", List.of(), List.of("session:")));
    }

    @Test
    void appliesTheDenyListBeforeTheOnlyList() {
        assertEquals(Optional.empty(),
                RedisKeyExistenceRouter.inScopeSourceKey("app:tmp:1", null, ":", List.of("app:tmp:"), List.of("app:")));
    }

    @Test
    void parsesACommaSeparatedPrefixListIgnoringBlanks() {
        assertEquals(List.of("a:", "b:"), RedisKeyExistenceRouter.parsePrefixList(" a: , , b: "));
        assertEquals(List.of(), RedisKeyExistenceRouter.parsePrefixList("  "));
    }
}
