package io.dragonfly.nifi.redis.services;

import io.dragonfly.nifi.redis.util.ClusterTopologySnapshot;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.nifi.controller.ControllerService;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Provides pooled access to a Redis-compatible endpoint via Lettuce. Implementations may be
 * backed by a single multiplexed connection or a pool of connections, depending on the
 * {@code Max Pool Size} property; callers must not assume either.
 */
public interface RedisConnectionPoolService extends ControllerService {

    /**
     * Runs {@code fn} against a borrowed async command interface, returning the connection
     * afterwards (including on exception). {@link RedisClusterAsyncCommands} is used as the
     * common command surface because it is implemented by both the standalone and cluster
     * Lettuce command types.
     */
    <T> T withConnection(Function<RedisClusterAsyncCommands<byte[], byte[]>, T> fn);

    /**
     * Like {@link #withConnection}, but hands back the raw connection instead of its async
     * command view - needed to dispatch module commands (JSON.*, TOPK.*) that Lettuce's typed
     * command interfaces don't expose, per {@code ModuleTypeHandler} (spec section 8.3).
     */
    <T> T withRawConnection(Function<StatefulConnection<byte[], byte[]>, T> fn);

    /**
     * Like {@link #withConnection} and {@link #withRawConnection} combined - both views are
     * derived from the same single borrowed connection (async command view and raw view), so
     * {@code fn} can dispatch typed commands and raw module commands (e.g. {@code JSON.SET} via
     * {@code RawModuleCommands}) against the same connection without borrowing twice. The
     * connection is released once, after {@code fn} returns (including on exception).
     */
    <T> T withConnectionAndRaw(BiFunction<RedisClusterAsyncCommands<byte[], byte[]>, StatefulConnection<byte[], byte[]>, T> fn);

    /** Whether this service is configured against a Redis Cluster (real or emulated) topology. */
    boolean isClusterMode();

    /** Opens a dedicated keyspace-notification subscription; the caller must {@link RedisPubSubHandle#close()} it. */
    RedisPubSubHandle openPubSub();

    /**
     * The serving masters and their hash slots as this service currently sees them, or empty
     * when the service is not in cluster mode and therefore has no topology to report.
     */
    Optional<ClusterTopologySnapshot> currentTopology();
}
