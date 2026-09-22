package io.dragonfly.nifi.redis.services;

import io.dragonfly.nifi.redis.util.ClusterTopologySnapshot;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Shared implementation backing both {@link RedisConnectionPoolService} (source) and
 * {@link DragonflyConnectionPoolService} (target). Kept as two distinct controller service
 * types per spec section 3.2 so migration source and target credentials, TLS configuration,
 * and provenance stay independent even though the underlying wiring is identical - both
 * Redis and Dragonfly speak the same wire protocol.
 */
public abstract class AbstractRedisConnectionPoolService extends AbstractControllerService {

    public enum ConnectionMode { STANDALONE, SENTINEL, CLUSTER }

    public static final PropertyDescriptor CONNECTION_MODE = new PropertyDescriptor.Builder()
            .name("connection-mode")
            .displayName("Connection Mode")
            .description("Redis topology mode. STANDALONE and SENTINEL both connect via a single "
                    + "Lettuce RedisClient (Lettuce auto-discovers the current master for a "
                    + "sentinel:// connection string); CLUSTER connects via RedisClusterClient "
                    + "against a comma-separated list of seed nodes.")
            .required(true)
            .allowableValues(ConnectionMode.STANDALONE.name(), ConnectionMode.SENTINEL.name(), ConnectionMode.CLUSTER.name())
            .defaultValue(ConnectionMode.STANDALONE.name())
            .build();

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("connection-string")
            .displayName("Connection String")
            .description("STANDALONE: redis://[:password@]host:port[/db] (or rediss:// for TLS). "
                    + "SENTINEL: redis-sentinel://[:password@]host1:port1,host2:port2/db#masterName. "
                    + "CLUSTER: comma-separated seed nodes, e.g. redis://host1:port1,redis://host2:port2.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor REQUIRE_TLS = new PropertyDescriptor.Builder()
            .name("require-tls")
            .displayName("Require TLS")
            .description("Whether connections must use TLS. Defaults to true; disabling requires "
                    + "explicit opt-in since unencrypted connections should never be used in production.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSLContextService used to configure TLS trust (and, optionally, a "
                    + "client keystore for mutual TLS). Required when Require TLS is true. Using "
                    + "NiFi's standard SSL Context Service keeps key material out of processor "
                    + "properties, flow.json.gz, and logs.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("connection-timeout-ms")
            .displayName("Connection Timeout (ms)")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5000")
            .build();

    public static final PropertyDescriptor COMMAND_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("command-timeout-ms")
            .displayName("Command Timeout (ms)")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();

    public static final PropertyDescriptor MAX_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("max-pool-size")
            .displayName("Max Pool Size")
            .description("Maximum number of pooled connections handed out concurrently per NiFi node.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("8")
            .build();

    public static final PropertyDescriptor DATABASE_INDEX = new PropertyDescriptor.Builder()
            .name("database-index")
            .displayName("Database Index")
            .description("SELECT index for STANDALONE/SENTINEL mode. Ignored for CLUSTER mode, "
                    + "which does not support multiple logical databases.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_MODE, CONNECTION_STRING, REQUIRE_TLS, SSL_CONTEXT_SERVICE,
            CONNECTION_TIMEOUT_MS, COMMAND_TIMEOUT_MS, MAX_POOL_SIZE, DATABASE_INDEX);

    private volatile boolean clusterMode;
    private RedisClient redisClient;
    private RedisClusterClient redisClusterClient;
    private GenericObjectPool<StatefulRedisConnection<byte[], byte[]>> standalonePool;
    private GenericObjectPool<StatefulRedisClusterConnection<byte[], byte[]>> clusterPool;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected java.util.Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();
        boolean requireTls = validationContext.getProperty(REQUIRE_TLS).asBoolean();
        if (requireTls && !validationContext.getProperty(SSL_CONTEXT_SERVICE).isSet()) {
            results.add(new ValidationResult.Builder()
                    .subject(SSL_CONTEXT_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("An SSL Context Service is required when Require TLS is true")
                    .build());
        }
        return results;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) throws InitializationException {
        ConnectionMode mode = ConnectionMode.valueOf(context.getProperty(CONNECTION_MODE).getValue());
        this.clusterMode = mode == ConnectionMode.CLUSTER;
        String connectionString = context.getProperty(CONNECTION_STRING).getValue();
        int databaseIndex = context.getProperty(DATABASE_INDEX).asInteger();
        Duration connectTimeout = Duration.ofMillis(context.getProperty(CONNECTION_TIMEOUT_MS).asLong());
        Duration commandTimeout = Duration.ofMillis(context.getProperty(COMMAND_TIMEOUT_MS).asLong());
        int maxPoolSize = context.getProperty(MAX_POOL_SIZE).asInteger();

        boolean requireTls = context.getProperty(REQUIRE_TLS).asBoolean();
        SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE)
                .asControllerService(SSLContextService.class);
        SslOptions sslOptions = requireTls ? buildSslOptions(sslContextService) : null;

        try {
            if (clusterMode) {
                List<RedisURI> seedUris = new ArrayList<>();
                for (String part : connectionString.split(",")) {
                    RedisURI uri = RedisURI.create(part.trim());
                    uri.setTimeout(commandTimeout);
                    uri.setSsl(requireTls);
                    seedUris.add(uri);
                }
                redisClusterClient = RedisClusterClient.create(seedUris);
                ClusterClientOptions.Builder optionsBuilder = ClusterClientOptions.builder()
                        .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
                                .enableAllAdaptiveRefreshTriggers()
                                .build());
                applySocketAndSsl(optionsBuilder, connectTimeout, sslOptions);
                redisClusterClient.setOptions(optionsBuilder.build());
                clusterPool = ConnectionPoolSupport.createGenericObjectPool(
                        () -> redisClusterClient.connect(ByteArrayCodec.INSTANCE),
                        poolConfig(maxPoolSize), true);
            } else {
                RedisURI uri = RedisURI.create(connectionString);
                uri.setTimeout(commandTimeout);
                uri.setSsl(requireTls);
                // This branch covers both STANDALONE and SENTINEL (CLUSTER took the other branch
                // above); DATABASE_INDEX's own description promises both, so it applies here
                // unconditionally.
                uri.setDatabase(databaseIndex);
                redisClient = RedisClient.create(uri);
                ClientOptions.Builder optionsBuilder = ClientOptions.builder();
                applySocketAndSsl(optionsBuilder, connectTimeout, sslOptions);
                redisClient.setOptions(optionsBuilder.build());
                standalonePool = ConnectionPoolSupport.createGenericObjectPool(
                        () -> redisClient.connect(ByteArrayCodec.INSTANCE),
                        poolConfig(maxPoolSize), true);
            }
        } catch (RuntimeException e) {
            // NiFi does not invoke @OnDisabled when @OnEnabled throws, so any client/pool already
            // built above (e.g. client created, then pool creation fails) must be released here.
            releaseResources();
            throw new InitializationException("Unable to establish Redis connection pool", e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        releaseResources();
    }

    private void releaseResources() {
        if (standalonePool != null) {
            standalonePool.close();
            standalonePool = null;
        }
        if (clusterPool != null) {
            clusterPool.close();
            clusterPool = null;
        }
        if (redisClient != null) {
            redisClient.shutdown();
            redisClient = null;
        }
        if (redisClusterClient != null) {
            redisClusterClient.shutdown();
            redisClusterClient = null;
        }
    }

    public <T> T withConnection(Function<RedisClusterAsyncCommands<byte[], byte[]>, T> fn) {
        if (clusterMode) {
            try (StatefulRedisClusterConnection<byte[], byte[]> connection = borrow(clusterPool)) {
                return fn.apply(connection.async());
            }
        } else {
            try (StatefulRedisConnection<byte[], byte[]> connection = borrow(standalonePool)) {
                return fn.apply(connection.async());
            }
        }
    }

    public <T> T withRawConnection(Function<StatefulConnection<byte[], byte[]>, T> fn) {
        if (clusterMode) {
            try (StatefulRedisClusterConnection<byte[], byte[]> connection = borrow(clusterPool)) {
                return fn.apply(connection);
            }
        } else {
            try (StatefulRedisConnection<byte[], byte[]> connection = borrow(standalonePool)) {
                return fn.apply(connection);
            }
        }
    }

    public <T> T withConnectionAndRaw(BiFunction<RedisClusterAsyncCommands<byte[], byte[]>, StatefulConnection<byte[], byte[]>, T> fn) {
        if (clusterMode) {
            try (StatefulRedisClusterConnection<byte[], byte[]> connection = borrow(clusterPool)) {
                return fn.apply(connection.async(), connection);
            }
        } else {
            try (StatefulRedisConnection<byte[], byte[]> connection = borrow(standalonePool)) {
                return fn.apply(connection.async(), connection);
            }
        }
    }

    public boolean isClusterMode() {
        return clusterMode;
    }

    /**
     * Reads Lettuce's own partition table, which this client keeps current through
     * {@code enableAllAdaptiveRefreshTriggers()} - no command is issued and no refresh is needed.
     */
    public Optional<ClusterTopologySnapshot> currentTopology() {
        if (!clusterMode) {
            return Optional.empty();
        }
        List<ClusterTopologySnapshot.MasterNode> masters = new ArrayList<>();
        for (RedisClusterNode node : redisClusterClient.getPartitions()) {
            if (!isServingMaster(node)) {
                continue;
            }
            masters.add(new ClusterTopologySnapshot.MasterNode(
                    node.getNodeId(), node.getUri().getHost(), node.getUri().getPort(), Set.copyOf(node.getSlots())));
        }
        return Optional.of(ClusterTopologySnapshot.of(System.currentTimeMillis(), masters));
    }

    /**
     * A master flagged FAIL/EVENTUAL_FAIL/HANDSHAKE/NOADDR is either not serving or has no usable
     * address, so counting it as current would report a spurious critical drift during a routine
     * failover. Slot count is deliberately not a filter: a slotless new master is still a master
     * the open subscription never subscribed to.
     */
    private static boolean isServingMaster(RedisClusterNode node) {
        return node.is(RedisClusterNode.NodeFlag.MASTER)
                && !node.is(RedisClusterNode.NodeFlag.FAIL)
                && !node.is(RedisClusterNode.NodeFlag.EVENTUAL_FAIL)
                && !node.is(RedisClusterNode.NodeFlag.HANDSHAKE)
                && !node.is(RedisClusterNode.NodeFlag.NOADDR);
    }

    /**
     * Cluster mode uses one {@code StatefulRedisClusterPubSubConnection} with node message
     * propagation enabled and subscribes on every master via {@code masters().commands()} -
     * this is Lettuce's built-in equivalent of the spec's "open a separate pub/sub connection
     * per shard master" instruction (section 4.4), achieved through one client-side connection
     * object and one listener registration instead of N manually managed connections.
     */
    public RedisPubSubHandle openPubSub() {
        return clusterMode ? openClusterPubSub() : openStandalonePubSub();
    }

    private RedisPubSubHandle openStandalonePubSub() {
        io.lettuce.core.pubsub.StatefulRedisPubSubConnection<byte[], byte[]> connection =
                redisClient.connectPubSub(ByteArrayCodec.INSTANCE);
        return new RedisPubSubHandle() {
            @Override
            public void psubscribe(String pattern, java.util.function.BiConsumer<byte[], byte[]> onMessage) {
                // A Lettuce pub/sub listener is connection-scoped, not subscription-scoped: it
                // fires for every message on this connection regardless of which psubscribe call
                // registered it. Registering one per call would deliver each message N times once
                // more than one pattern is subscribed on the same handle.
                connection.addListener(new io.lettuce.core.pubsub.RedisPubSubAdapter<byte[], byte[]>() {
                    @Override
                    public void message(byte[] pattern, byte[] channel, byte[] message) {
                        onMessage.accept(channel, message);
                    }
                });
                connection.async().psubscribe(pattern.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }

            @Override
            public void onConnectionStateChange(Runnable onDisconnected, Runnable onReconnected) {
                addConnectionStateListener(connection, onDisconnected, onReconnected);
            }

            @Override
            public void close() {
                connection.close();
            }
        };
    }

    private RedisPubSubHandle openClusterPubSub() {
        io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection<byte[], byte[]> connection =
                redisClusterClient.connectPubSub(ByteArrayCodec.INSTANCE);
        connection.setNodeMessagePropagation(true);
        return new RedisPubSubHandle() {
            @Override
            public void psubscribe(String pattern, java.util.function.BiConsumer<byte[], byte[]> onMessage) {
                connection.addListener(new io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter<byte[], byte[]>() {
                    @Override
                    public void message(io.lettuce.core.cluster.models.partitions.RedisClusterNode node, byte[] pattern, byte[] channel, byte[] message) {
                        onMessage.accept(channel, message);
                    }
                });
                connection.async().masters().commands().psubscribe(pattern.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }

            @Override
            public void onConnectionStateChange(Runnable onDisconnected, Runnable onReconnected) {
                addConnectionStateListener(connection, onDisconnected, onReconnected);
            }

            @Override
            public void close() {
                connection.close();
            }
        };
    }

    private static void addConnectionStateListener(StatefulConnection<byte[], byte[]> connection, Runnable onDisconnected, Runnable onReconnected) {
        connection.addListener(new io.lettuce.core.RedisConnectionStateListener() {
            @Override
            public void onRedisDisconnected(io.lettuce.core.RedisChannelHandler<?, ?> handler) {
                onDisconnected.run();
            }

            // Lettuce may invoke either onRedisConnected overload on reconnect; both are wired
            // because the handler is idempotent under a double call.
            @Override
            public void onRedisConnected(io.lettuce.core.RedisChannelHandler<?, ?> handler) {
                onReconnected.run();
            }

            @Override
            public void onRedisConnected(io.lettuce.core.RedisChannelHandler<?, ?> handler, java.net.SocketAddress socketAddress) {
                onReconnected.run();
            }
        });
    }

    private static <T extends StatefulConnection<byte[], byte[]>> T borrow(GenericObjectPool<T> pool) {
        try {
            return pool.borrowObject();
        } catch (Exception e) {
            throw new RedisConnectionPoolException("Unable to borrow a connection from the pool", e);
        }
    }

    private static <T> GenericObjectPoolConfig<T> poolConfig(int maxPoolSize) {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(maxPoolSize);
        config.setMaxIdle(maxPoolSize);
        config.setTestOnBorrow(true);
        return config;
    }

    private static void applySocketAndSsl(ClientOptions.Builder builder, Duration connectTimeout, SslOptions sslOptions) {
        builder.socketOptions(io.lettuce.core.SocketOptions.builder().connectTimeout(connectTimeout).build());
        if (sslOptions != null) {
            builder.sslOptions(sslOptions);
        }
    }

    /**
     * Bridges NiFi's SSLContextService (keystore/truststore file + password + type) into
     * Lettuce's SslOptions, which only accepts key/trust material as files or factories - not
     * as a pre-built SSLContext. Lettuce's SslOptions has a single shared keystore-type setter,
     * so a keystore and truststore of different types cannot both be honored; this is a Lettuce
     * limitation, not a NiFi one.
     */
    private static SslOptions buildSslOptions(SSLContextService sslContextService) throws InitializationException {
        if (sslContextService == null) {
            throw new InitializationException("Require TLS is true but no SSL Context Service is configured");
        }
        SslOptions.Builder builder = SslOptions.builder();
        if (sslContextService.isTrustStoreConfigured()) {
            builder.keyStoreType(sslContextService.getTrustStoreType());
            builder.truststore(new File(sslContextService.getTrustStoreFile()), sslContextService.getTrustStorePassword());
        }
        if (sslContextService.isKeyStoreConfigured()) {
            builder.keyStoreType(sslContextService.getKeyStoreType());
            builder.keystore(new File(sslContextService.getKeyStoreFile()), sslContextService.getKeyStorePassword().toCharArray());
        }
        return builder.build();
    }
}
