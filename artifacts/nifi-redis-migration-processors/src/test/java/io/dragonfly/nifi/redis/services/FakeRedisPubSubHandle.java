package io.dragonfly.nifi.redis.services;

import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

/**
 * In-memory {@link RedisPubSubHandle} test double. It performs no network I/O; it captures the
 * callbacks a processor registers so a test can fire them directly.
 */
public class FakeRedisPubSubHandle implements RedisPubSubHandle {

    private volatile BiConsumer<byte[], byte[]> onMessage;
    private volatile Runnable onDisconnected;
    private volatile Runnable onReconnected;

    @Override
    public void psubscribe(String pattern, BiConsumer<byte[], byte[]> onMessage) {
        this.onMessage = onMessage;
    }

    @Override
    public void onConnectionStateChange(Runnable onDisconnected, Runnable onReconnected) {
        this.onDisconnected = onDisconnected;
        this.onReconnected = onReconnected;
    }

    @Override
    public void close() {
    }

    public void deliver(String channel, String message) {
        registered(onMessage, "psubscribe").accept(
                channel.getBytes(StandardCharsets.UTF_8), message.getBytes(StandardCharsets.UTF_8));
    }

    public void simulateDisconnect() {
        registered(onDisconnected, "onConnectionStateChange").run();
    }

    public void simulateReconnect() {
        registered(onReconnected, "onConnectionStateChange").run();
    }

    private static <T> T registered(T callback, String registrar) {
        if (callback == null) {
            throw new IllegalStateException("No callback registered; " + registrar + " has not been called on this handle");
        }
        return callback;
    }
}
