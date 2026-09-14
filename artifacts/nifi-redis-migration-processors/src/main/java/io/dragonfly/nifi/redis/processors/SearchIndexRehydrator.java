package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.DragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.util.RawModuleCommands;
import io.dragonfly.nifi.redis.util.SearchIndexCache;
import io.dragonfly.nifi.redis.util.SearchIndexDefinition;
import io.dragonfly.nifi.redis.util.UnsupportedSearchIndexException;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Runs once, when started: reads back the search index definitions {@link SearchIndexExporter}
 * cached for this migration id and rebuilds each one on the target via {@code FT.CREATE},
 * removing it from the cache once successfully created (or already existing) - a failure leaves
 * it in the cache for a retry on the next start, instead of losing the definition.
 *
 * <p>run-r2dfly.sh starts this only after confirming the main scan/write flow has actually
 * finished (DBSIZE polling + NiFi idle, same signal the original script-based rehydration step
 * waited for) - unlike export, "the migration is done" isn't something this processor (or NiFi
 * generally) can determine on its own for a filtered/partial migration, so it has to stay an
 * externally-triggered action. {@link #onTrigger} never does anything; all the work happens in
 * {@code @OnScheduled}, exactly once per start.
 *
 * <p>In cluster mode, {@code FT.CREATE} is broadcast to every master node (via {@link
 * StatefulRedisClusterConnection#getConnection(String)} per {@link
 * StatefulRedisClusterConnection#getPartitions()} entry) - a search index must exist on every
 * shard to cover documents living on any of them, unlike a plain key.
 */
@Tags({"redis", "dragonfly", "migration", "search", "ft.create"})
@CapabilityDescription("Runs once when started: rebuilds every search index SearchIndexExporter cached for "
        + "this migration id on the target via FT.CREATE (including VECTOR fields), broadcasting to every "
        + "master node in cluster mode. Not part of the per-key scan/write pipeline - start this only after "
        + "confirming the main flow has finished.")
public class SearchIndexRehydrator extends AbstractProcessor {

    public static final PropertyDescriptor TARGET_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("target-connection-pool")
            .displayName("Target Connection Pool")
            .required(true)
            .identifiesControllerService(DragonflyConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor CURSOR_STATE_CACHE = new PropertyDescriptor.Builder()
            .name("cursor-state-cache")
            .displayName("Cursor State Cache")
            .description("Must be the same controller service instance SearchIndexExporter was configured "
                    + "with - this is purely a hand-off channel, not this processor's own cursor state.")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor MIGRATION_ID = new PropertyDescriptor.Builder()
            .name("migration-id")
            .displayName("Migration ID")
            .description("Must match the migration id SearchIndexExporter was run with.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FT_CREATE_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("ft-create-timeout-ms")
            .displayName("FT.CREATE Timeout (ms)")
            .description("Max time to await each individual FT.CREATE call (one per index per master node).")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("30000")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
            List.of(TARGET_CONNECTION_POOL, CURSOR_STATE_CACHE, MIGRATION_ID, FT_CREATE_TIMEOUT_MS);

    // No FlowFiles are ever produced - this processor's only effect is FT.CREATE calls issued,
    // and cache entries removed, in @OnScheduled.
    private static final Set<Relationship> RELATIONSHIPS = Set.of();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws ProcessException {
        DragonflyConnectionPoolService targetPool = context.getProperty(TARGET_CONNECTION_POOL).asControllerService(DragonflyConnectionPoolService.class);
        DistributedMapCacheClient cache = context.getProperty(CURSOR_STATE_CACHE).asControllerService(DistributedMapCacheClient.class);
        String migrationId = context.getProperty(MIGRATION_ID).getValue();
        long timeoutMs = context.getProperty(FT_CREATE_TIMEOUT_MS).asLong();
        ComponentLog logger = getLogger();

        List<String> names;
        try {
            names = cache.get(SearchIndexCache.manifestKey(migrationId), SearchIndexCache.KEY_SERIALIZER, SearchIndexCache.NAMES_DESERIALIZER);
        } catch (IOException e) {
            throw new ProcessException("Unable to read search index manifest for migration '" + migrationId + "'", e);
        }
        if (names == null || names.isEmpty()) {
            logger.info("No cached search index definitions for migration '{}' - nothing to rehydrate", migrationId);
            return;
        }

        List<String> remaining = new ArrayList<>();
        for (String name : names) {
            SearchIndexDefinition def;
            try {
                def = cache.get(SearchIndexCache.definitionKey(migrationId, name), SearchIndexCache.KEY_SERIALIZER, SearchIndexCache.DEFINITION_DESERIALIZER);
            } catch (IOException e) {
                logger.warn("Could not read cached definition for index '{}' - will retry next start", name, e);
                remaining.add(name);
                continue;
            }
            if (def == null) {
                logger.warn("Manifest listed index '{}' for migration '{}' but its definition is missing from the cache - skipping", name, migrationId);
                continue;
            }

            List<byte[]> args;
            try {
                args = def.toFtCreateArgs();
            } catch (UnsupportedSearchIndexException e) {
                logger.warn("Skipping search index '{}' - not migrated: {}", name, e.getMessage());
                continue;
            }

            boolean ok = targetPool.isClusterMode()
                    ? createOnEveryMaster(targetPool, args, name, timeoutMs, logger)
                    : targetPool.withRawConnection(conn -> issueFtCreate(conn, args, name, "target", timeoutMs, logger));
            if (!ok) {
                remaining.add(name);
            }
        }

        try {
            if (remaining.isEmpty()) {
                cache.remove(SearchIndexCache.manifestKey(migrationId), SearchIndexCache.KEY_SERIALIZER);
            } else {
                cache.put(SearchIndexCache.manifestKey(migrationId), remaining, SearchIndexCache.KEY_SERIALIZER, SearchIndexCache.NAMES_SERIALIZER);
            }
            for (String name : names) {
                if (!remaining.contains(name)) {
                    cache.remove(SearchIndexCache.definitionKey(migrationId, name), SearchIndexCache.KEY_SERIALIZER);
                }
            }
        } catch (IOException e) {
            logger.warn("Failed to update search-index cache bookkeeping after rehydration for migration '{}'", migrationId, e);
        }
        logger.info("Rebuilt {} of {} search index(es) for migration '{}'{}", names.size() - remaining.size(), names.size(), migrationId,
                remaining.isEmpty() ? "" : " (" + remaining.size() + " left for retry)");
    }

    private boolean createOnEveryMaster(DragonflyConnectionPoolService targetPool, List<byte[]> args, String name, long timeoutMs, ComponentLog logger) {
        return targetPool.withRawConnection(conn -> {
            if (!(conn instanceof StatefulRedisClusterConnection<byte[], byte[]> clusterConn)) {
                logger.warn("Target connection pool reports cluster mode but its raw connection isn't a cluster connection - cannot broadcast FT.CREATE for index {}", name);
                return false;
            }
            boolean allOk = true;
            for (RedisClusterNode node : clusterConn.getPartitions()) {
                if (!node.is(RedisClusterNode.NodeFlag.MASTER)) {
                    continue;
                }
                StatefulRedisConnection<byte[], byte[]> nodeConn;
                try {
                    nodeConn = clusterConn.getConnection(node.getNodeId());
                } catch (Exception e) {
                    logger.warn("Could not reach master {} for index '{}'", node.getNodeId(), name, e);
                    allOk = false;
                    continue;
                }
                if (!issueFtCreate(nodeConn, args, name, node.getNodeId(), timeoutMs, logger)) {
                    allOk = false;
                }
            }
            return allOk;
        });
    }

    /** Issues one FT.CREATE and interprets the reply/exception - "OK" and "Index already exists"
     * both count as success (the latter leaves the pre-existing index as-is, matching the
     * original script's behavior), anything else is a real failure. */
    private static boolean issueFtCreate(StatefulConnection<byte[], byte[]> conn, List<byte[]> args, String name,
                                          String nodeLabel, long timeoutMs, ComponentLog logger) {
        try {
            String reply = RawModuleCommands.ftCreate(conn, args).get(timeoutMs, TimeUnit.MILLISECONDS);
            if (reply != null && reply.startsWith("OK")) {
                return true;
            }
            logger.warn("FAILED to create index '{}' on {}: unexpected reply '{}'", name, nodeLabel, reply);
            return false;
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            String message = cause.getMessage() == null ? "" : cause.getMessage();
            if (message.contains("Index already exists")) {
                logger.info("Index '{}' already exists on {} - leaving it as-is", name, nodeLabel);
                return true;
            }
            logger.warn("FAILED to create index '{}' on {}: {}", name, nodeLabel, message, cause);
            return false;
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        // All real work already happened in @OnScheduled, exactly once - nothing left to do
        // per-trigger, ever.
        context.yield();
    }
}
