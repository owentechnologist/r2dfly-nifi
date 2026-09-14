package io.dragonfly.nifi.redis.services;

import java.util.function.BiConsumer;

/**
 * A long-lived keyspace-notification subscription, distinct from {@link RedisConnectionPoolService#withConnection}'s
 * borrow-and-return model since pub/sub needs a dedicated connection with a registered listener
 * for the lifetime of the subscription.
 */
public interface RedisPubSubHandle extends AutoCloseable {

    /** Subscribes to {@code pattern}; {@code onMessage} is invoked with (channel, message) for each event. */
    void psubscribe(String pattern, BiConsumer<byte[], byte[]> onMessage);

    @Override
    void close();
}
