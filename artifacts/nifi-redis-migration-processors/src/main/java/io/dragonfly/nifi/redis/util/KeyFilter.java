package io.dragonfly.nifi.redis.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The key-type and key-prefix filters, shared by the scan path (RedisScanReader) and the live path
 * (RedisKeyspaceEventConsumer for prefixes, RedisSingleKeyFetch for types) so both phases select
 * exactly the same keys. An empty type set allows every type.
 */
public record KeyFilter(Set<String> allowedTypes, List<byte[]> denyPrefixes, List<byte[]> onlyPrefixes) {

    public static KeyFilter parse(String typeCsv, String denyCsv, String onlyCsv) {
        return new KeyFilter(parseTypes(typeCsv), parsePrefixes(denyCsv), parsePrefixes(onlyCsv));
    }

    /** Deny is checked before only, matching the documented order of the two prefix lists. */
    public boolean allowsKey(byte[] key) {
        if (startsWithAny(key, denyPrefixes)) {
            return false;
        }
        return onlyPrefixes.isEmpty() || startsWithAny(key, onlyPrefixes);
    }

    public boolean allowsKey(String key) {
        return allowsKey(key.getBytes(StandardCharsets.UTF_8));
    }

    /** Case-insensitive: TYPE answers module types in mixed case (ReJSON-RL, TopK-TYPE). */
    public boolean allowsType(String type) {
        return allowedTypes.isEmpty() || allowedTypes.contains(type.toLowerCase());
    }

    private static Set<String> parseTypes(String csv) {
        return splitCsv(csv).stream().map(String::toLowerCase).collect(Collectors.toUnmodifiableSet());
    }

    private static List<byte[]> parsePrefixes(String csv) {
        return splitCsv(csv).stream().map(s -> s.getBytes(StandardCharsets.UTF_8)).toList();
    }

    private static List<String> splitCsv(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    private static boolean startsWithAny(byte[] key, List<byte[]> prefixes) {
        for (byte[] prefix : prefixes) {
            if (key.length >= prefix.length && Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length)) {
                return true;
            }
        }
        return false;
    }
}
