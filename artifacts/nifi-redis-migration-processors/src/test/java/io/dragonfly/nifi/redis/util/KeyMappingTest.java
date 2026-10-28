package io.dragonfly.nifi.redis.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyMappingTest {

    @Test
    void renamesByTheLongestOldPrefixAndPutsKeyPrefixInFront() {
        KeyMapping mapping = KeyMapping.of("user:=account:, user:admin:=staff:", "app", ":");

        assertEquals(Optional.of("app:account:7"), target(mapping, "user:7"));
        assertEquals(Optional.of("app:staff:1"), target(mapping, "user:admin:1"));
        assertEquals(Optional.of("app:order:9"), target(mapping, "order:9"));
    }

    @Test
    void mapsTargetKeysBackOnlyWhenASourceKeyLandsThere() {
        KeyMapping mapping = KeyMapping.of("user:=account:,user:admin:=staff:", null, ":");

        assertEquals(Optional.of("user:7"), source(mapping, "account:7"));
        assertEquals(Optional.of("order:9"), source(mapping, "order:9"));
        assertEquals(Optional.empty(), source(mapping, "user:7"), "a user: key on the target is not a copy of anything");
        assertEquals(Optional.empty(), source(mapping, "account:admin:1"), "user:admin:1 goes to staff:, not account:");
    }

    @Test
    void refusesASourceKeyARenamedKeyWouldOverwrite() {
        KeyMapping mapping = KeyMapping.of("user:=account:", null, null);

        assertEquals(Optional.empty(), target(mapping, "account:7"));
        assertEquals(Optional.of("user:7"), source(mapping, "account:7"));
    }

    @Test
    void swapsTwoPrefixes() {
        KeyMapping mapping = KeyMapping.of("a:=b:,b:=a:", null, null);

        assertEquals(Optional.of("b:1"), target(mapping, "a:1"));
        assertEquals(Optional.of("a:1"), target(mapping, "b:1"));
        assertEquals(Optional.of("b:1"), source(mapping, "a:1"));
    }

    @Test
    void anEmptyOldPrefixMatchesEveryKey() {
        KeyMapping mapping = KeyMapping.of("=v2:", null, null);

        assertEquals(Optional.of("v2:x"), target(mapping, "x"));
        assertEquals(Optional.of("v2:v2:x"), target(mapping, "v2:x"));
        assertEquals(Optional.of("x"), source(mapping, "v2:x"));
        assertEquals(Optional.empty(), source(mapping, "x"));
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
    void renamesABinaryKeyByItsBytes() {
        KeyMapping mapping = KeyMapping.of("user:=account:", null, null);
        byte[] binary = {'u', 's', 'e', 'r', ':', (byte) 0xff, 0x00, (byte) 0xc3};

        byte[] renamed = mapping.toTarget(binary).orElseThrow();

        assertEquals("account:", new String(renamed, 0, 8, StandardCharsets.UTF_8));
        assertTrue(java.util.Arrays.equals(new byte[] {(byte) 0xff, 0x00, (byte) 0xc3}, java.util.Arrays.copyOfRange(renamed, 8, renamed.length)));
        assertTrue(java.util.Arrays.equals(binary, mapping.toSource(renamed).orElseThrow()));
    }

    private static Optional<String> target(KeyMapping mapping, String key) {
        return mapping.toTarget(key.getBytes(StandardCharsets.UTF_8)).map(k -> new String(k, StandardCharsets.UTF_8));
    }

    private static Optional<String> source(KeyMapping mapping, String key) {
        return mapping.toSource(key.getBytes(StandardCharsets.UTF_8)).map(k -> new String(k, StandardCharsets.UTF_8));
    }

    @Test
    void noRulesAndNoPrefixIsTheIdentity() {
        assertTrue(KeyMapping.of(" ", "", ":").isIdentity());
        assertEquals(Optional.of("k"), source(KeyMapping.identity(), "k"));
    }
}
