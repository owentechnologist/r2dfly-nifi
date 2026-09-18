package io.dragonfly.nifi.redis.services;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.PipelinedRedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.nifi.controller.AbstractControllerService;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * In-memory {@link RedisConnectionPoolService} test double, backed by a {@link FakeRedisPubSubHandle}
 * instead of a Redis endpoint. Extends {@link AbstractControllerService} so {@code TestRunner}
 * accepts it as a controller service.
 */
public class FakeRedisConnectionPoolService extends AbstractControllerService implements RedisConnectionPoolService {

    /** The minimum {@code notify-keyspace-events} setting keyspace consumers validate against. */
    private static final String NOTIFY_KEYSPACE_EVENTS = "AE";

    private final FakeRedisPubSubHandle handle = new FakeRedisPubSubHandle();

    public FakeRedisPubSubHandle handle() {
        return handle;
    }

    /**
     * There is no mocking framework on this classpath, so the command surface is a reflective
     * proxy serving only the one call keyspace consumers make against it.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T withConnection(Function<RedisClusterAsyncCommands<byte[], byte[]>, T> fn) {
        RedisClusterAsyncCommands<byte[], byte[]> commands = (RedisClusterAsyncCommands<byte[], byte[]>) Proxy.newProxyInstance(
                FakeRedisConnectionPoolService.class.getClassLoader(),
                new Class<?>[]{RedisClusterAsyncCommands.class},
                (proxy, method, args) -> {
                    if ("configGet".equals(method.getName())) {
                        return new PipelinedRedisFuture<>(CompletableFuture.completedFuture(
                                Map.of("notify-keyspace-events", NOTIFY_KEYSPACE_EVENTS)));
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        return fn.apply(commands);
    }

    @Override
    public <T> T withRawConnection(Function<StatefulConnection<byte[], byte[]>, T> fn) {
        throw new UnsupportedOperationException("withRawConnection");
    }

    @Override
    public boolean isClusterMode() {
        return false;
    }

    @Override
    public RedisPubSubHandle openPubSub() {
        return handle;
    }
}
