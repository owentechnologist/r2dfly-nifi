package io.dragonfly.nifi.redis.processors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RedisKeyspaceEventConsumerTest {

    @Test
    void parsesAKeyeventChannel() {
        RedisKeyspaceEventConsumer.ParsedEvent event = RedisKeyspaceEventConsumer.parse("__keyevent@0__:set", "user:session:abc123");

        assertEquals("user:session:abc123", event.key());
        assertEquals("set", event.eventType());
        assertEquals(0, event.database());
    }

    @Test
    void parsesAKeyspaceChannel() {
        RedisKeyspaceEventConsumer.ParsedEvent event = RedisKeyspaceEventConsumer.parse("__keyspace@3__:user:session:abc123", "del");

        assertEquals("user:session:abc123", event.key());
        assertEquals("del", event.eventType());
        assertEquals(3, event.database());
    }

    @Test
    void returnsNullForAnUnrecognizedChannel() {
        assertNull(RedisKeyspaceEventConsumer.parse("some.other.channel", "payload"));
    }
}
