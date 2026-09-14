package io.dragonfly.nifi.redis.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Shared key-naming and (de)serialization for handing {@link SearchIndexDefinition}s from {@code
 * SearchIndexExporter} to {@code SearchIndexRehydrator} through the same {@code
 * DistributedMapCacheClient} (Cursor State Cache) {@code RedisScanReader}/{@code
 * PartitionAssigner} already use for partition-claim/checkpoint state - not the source/target
 * Redis keyspace itself. This avoids the original Lua-based design's whole reason for existing
 * (smuggling definitions through NiFi's own string-key pipeline as an ordinary key): the
 * definitions now never touch the source or target database at all, so there's no temp-key
 * prefix, no cluster-mode "which shard did it land on" discovery scan, and nothing left over to
 * clean up if a migration is abandoned mid-way (the cache entry just sits unread, exactly like an
 * abandoned partition-claim would).
 *
 * <p>A single manifest entry per migration id lists which index names were exported (since {@code
 * DistributedMapCacheClient} only supports get/put/remove by exact key, not listing/scanning -
 * see {@link PartitionAssigner}'s own doc comment on the same limitation), so the rehydrator knows
 * which per-index keys to then read.
 */
public final class SearchIndexCache {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final Serializer<String> KEY_SERIALIZER = (value, out) -> out.write(value.getBytes(StandardCharsets.UTF_8));

    public static final Serializer<SearchIndexDefinition> DEFINITION_SERIALIZER =
            (value, out) -> OBJECT_MAPPER.writeValue(out, value);

    public static final Deserializer<SearchIndexDefinition> DEFINITION_DESERIALIZER =
            bytes -> (bytes == null || bytes.length == 0) ? null : OBJECT_MAPPER.readValue(bytes, SearchIndexDefinition.class);

    public static final Serializer<List<String>> NAMES_SERIALIZER = (value, out) -> OBJECT_MAPPER.writeValue(out, value);

    public static final Deserializer<List<String>> NAMES_DESERIALIZER =
            bytes -> (bytes == null || bytes.length == 0) ? null : OBJECT_MAPPER.readValue(bytes, new TypeReference<List<String>>() {
            });

    private SearchIndexCache() {
    }

    public static String manifestKey(String migrationId) {
        return "search-index-def." + migrationId + ".names";
    }

    public static String definitionKey(String migrationId, String indexName) {
        return "search-index-def." + migrationId + "." + indexName;
    }
}
