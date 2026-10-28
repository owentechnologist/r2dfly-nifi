package io.dragonfly.nifi.redis.util;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyMappingTest {

    @Test
    void renamesByTheLongestOldPrefixAndPutsKeyPrefixInFront() {
        KeyMapping mapping = KeyMapping.of("user:=account:, user:admin:=staff:", "app", ":");

        assertEquals(Optional.of("app:account:7"), mapping.toTarget("user:7"));
        assertEquals(Optional.of("app:staff:1"), mapping.toTarget("user:admin:1"));
        assertEquals(Optional.of("app:order:9"), mapping.toTarget("order:9"));
    }

    @Test
    void mapsTargetKeysBackOnlyWhenASourceKeyLandsThere() {
        KeyMapping mapping = KeyMapping.of("user:=account:,user:admin:=staff:", null, ":");

        assertEquals(Optional.of("user:7"), mapping.toSource("account:7"));
        assertEquals(Optional.of("order:9"), mapping.toSource("order:9"));
        assertEquals(Optional.empty(), mapping.toSource("user:7"), "a user: key on the target is not a copy of anything");
        assertEquals(Optional.empty(), mapping.toSource("account:admin:1"), "user:admin:1 goes to staff:, not account:");
    }

    @Test
    void refusesASourceKeyARenamedKeyWouldOverwrite() {
        KeyMapping mapping = KeyMapping.of("user:=account:", null, null);

        assertEquals(Optional.empty(), mapping.toTarget("account:7"));
        assertEquals(Optional.of("user:7"), mapping.toSource("account:7"));
    }

    @Test
    void swapsTwoPrefixes() {
        KeyMapping mapping = KeyMapping.of("a:=b:,b:=a:", null, null);

        assertEquals(Optional.of("b:1"), mapping.toTarget("a:1"));
        assertEquals(Optional.of("a:1"), mapping.toTarget("b:1"));
        assertEquals(Optional.of("b:1"), mapping.toSource("a:1"));
    }

    @Test
    void anEmptyOldPrefixMatchesEveryKey() {
        KeyMapping mapping = KeyMapping.of("=v2:", null, null);

        assertEquals(Optional.of("v2:x"), mapping.toTarget("x"));
        assertEquals(Optional.of("v2:v2:x"), mapping.toTarget("v2:x"));
        assertEquals(Optional.of("x"), mapping.toSource("v2:x"));
        assertEquals(Optional.empty(), mapping.toSource("x"));
    }

    @Test
    void mapsAnIndexPrefixToEveryTargetPrefixItsKeysReach() {
        KeyMapping mapping = KeyMapping.of("user:=account:", "app", ":");

        assertEquals(List.of("app:account:"), mapping.targetPrefixes("user:"));
        assertEquals(List.of("app:u", "app:account:"), mapping.targetPrefixes("u"));
        assertEquals(List.of("app:order:"), mapping.targetPrefixes("order:"));
    }

    @Test
    void rejectsRulesThatCannotBeUndone() {
        assertTrue(assertThrows(IllegalArgumentException.class, () -> KeyMapping.of("a:=x:,b:=x:y:", null, null))
                .getMessage().contains("overlap"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> KeyMapping.of("a:=x:,a:=y:", null, null))
                .getMessage().contains("twice"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> KeyMapping.of("a:=", null, null))
                .getMessage().contains("no new prefix"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> KeyMapping.of("a:", null, null))
                .getMessage().contains("not old=new"));
    }

    @Test
    void noRulesAndNoPrefixIsTheIdentity() {
        assertTrue(KeyMapping.of(" ", "", ":").isIdentity());
        assertEquals(Optional.of("k"), KeyMapping.identity().toSource("k"));
    }
}
