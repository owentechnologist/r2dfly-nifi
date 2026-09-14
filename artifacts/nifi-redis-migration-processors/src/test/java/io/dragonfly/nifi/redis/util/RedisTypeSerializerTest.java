package io.dragonfly.nifi.redis.util;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RedisTypeSerializerTest {

    @Test
    void roundTripsAStringRecord() throws Exception {
        KeyRecord original = new KeyRecord("greeting", "string", 60_000, "embstr", "hello world");
        byte[] json = RedisTypeSerializer.toJsonBytes(original);
        KeyRecord restored = RedisTypeSerializer.readJson(json);

        assertEquals(original.key, restored.key);
        assertEquals(original.type, restored.type);
        assertEquals(original.ttlMs, restored.ttlMs);
        assertEquals(original.encoding, restored.encoding);
        assertEquals(original.value, restored.value);
    }

    @Test
    void roundTripsAHashRecordAsAMap() throws Exception {
        KeyRecord original = new KeyRecord("user:1", "hash", -1, null, Map.of("name", "Owen", "role", "eng"));
        byte[] json = RedisTypeSerializer.toJsonBytes(original);
        KeyRecord restored = RedisTypeSerializer.readJson(json);

        assertEquals(Map.of("name", "Owen", "role", "eng"), restored.value);
    }

    @Test
    void roundTripsAZsetRecordAsAListOfMemberScoreEntries() throws Exception {
        KeyRecord original = new KeyRecord("leaderboard", "zset", -1, null,
                List.of(Map.of("member", "alice", "score", 1.5), Map.of("member", "bob", "score", 2.0)));
        byte[] json = RedisTypeSerializer.toJsonBytes(original);
        KeyRecord restored = RedisTypeSerializer.readJson(json);

        assertEquals(original.value, restored.value);
    }

    @Test
    void omitsNullEncodingAndConsumerGroupsFromJson() throws Exception {
        KeyRecord record = new KeyRecord("k", "string", -1, null, "v");
        byte[] json = RedisTypeSerializer.toJsonBytes(record);
        String jsonText = new String(json);

        assertEquals(false, jsonText.contains("encoding"));
        assertEquals(false, jsonText.contains("consumer_groups"));
        assertNull(RedisTypeSerializer.readJson(json).consumerGroups);
    }
}
