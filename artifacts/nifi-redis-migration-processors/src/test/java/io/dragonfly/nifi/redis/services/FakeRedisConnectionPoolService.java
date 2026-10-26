package io.dragonfly.nifi.redis.services;

import io.dragonfly.nifi.redis.util.ClusterTopologySnapshot;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.PipelinedRedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.nifi.controller.AbstractControllerService;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * In-memory {@link RedisConnectionPoolService} test double, backed by a {@link FakeRedisPubSubHandle}
 * instead of a Redis endpoint. Extends {@link AbstractControllerService} so {@code TestRunner}
 * accepts it as a controller service.
 */
public class FakeRedisConnectionPoolService extends AbstractControllerService implements RedisConnectionPoolService {

    /** Defaults to the minimum setting keyspace consumers accept; null makes CONFIG GET fail, as on ElastiCache. */
    private volatile String notifyKeyspaceEvents = "AE";

    private final FakeRedisPubSubHandle handle = new FakeRedisPubSubHandle();

    /** Unset until a test calls {@link #setTopology}, so the fake reports as non-cluster by default. */
    private volatile ClusterTopologySnapshot topology;

    public FakeRedisPubSubHandle handle() {
        return handle;
    }

    public void setNotifyKeyspaceEvents(String value) {
        this.notifyKeyspaceEvents = value;
    }

    public void setTopology(ClusterTopologySnapshot topology) {
        this.topology = topology;
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
                        String value = notifyKeyspaceEvents;
                        return new PipelinedRedisFuture<>(value == null
                                ? CompletableFuture.failedFuture(new IllegalStateException("ERR unknown command 'CONFIG'"))
                                : CompletableFuture.completedFuture(Map.of("notify-keyspace-events", value)));
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
    public <T> T withConnectionAndRaw(BiFunction<RedisClusterAsyncCommands<byte[], byte[]>, StatefulConnection<byte[], byte[]>, T> fn) {
        throw new UnsupportedOperationException("withConnectionAndRaw");
    }

    @Override
    public boolean isClusterMode() {
        return false;
    }

    @Override
    public RedisPubSubHandle openPubSub() {
        return handle;
    }

    @Override
    public Optional<ClusterTopologySnapshot> currentTopology() {
        return Optional.ofNullable(topology);
    }
}
