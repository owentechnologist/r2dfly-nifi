package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.RawModuleCommands;
import io.dragonfly.nifi.redis.util.SearchIndexCache;
import io.dragonfly.nifi.redis.util.SearchIndexDefinition;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Runs once, when started: enumerates every search index on the source via {@code FT._LIST}/
 * {@code FT.INFO}, normalizes each into a {@link SearchIndexDefinition}, and caches them
 * (namespaced by migration id) in the shared Cursor State Cache for {@link
 * SearchIndexRehydrator} to rebuild on the target later via {@code FT.CREATE}.
 *
 * <p>Deliberately not part of the per-key scan/write pipeline - index definitions aren't
 * keyspace keys {@code RedisScanReader} could ever discover via {@code SCAN}, and this is a
 * whole-catalog operation, not a per-key one. run-r2dfly.sh starts this processor before the main
 * flow (so definitions are captured before anything else runs) and stops it again once {@code
 * @OnScheduled} completes - all the real work happens there, exactly once; {@link
 * #onTrigger} never does anything, since there's no per-FlowFile work to do.
 *
 * <p>Caches into the same {@code DistributedMapCacheClient} {@code RedisScanReader}/{@code
 * PartitionAssigner} already use for partition-claim/checkpoint state, not the source/target
 * Redis keyspace itself - see {@link SearchIndexCache}'s own doc comment for why that's a real
 * improvement over the project's original temp-key-in-the-actual-database design, not just a
 * different way to do the same thing.
 */
@Tags({"redis", "dragonfly", "migration", "search", "ft.create", "ft.info"})
@CapabilityDescription("Runs once when started: enumerates every search index on the source via FT._LIST/"
        + "FT.INFO (including VECTOR fields) and caches a normalized definition of each one, namespaced by "
        + "migration id, for SearchIndexRehydrator to rebuild on the target later. Not part of the per-key "
        + "scan/write pipeline - start this before the main flow, stop it again once it reaches RUNNING.")
public class SearchIndexExporter extends AbstractProcessor {

    public static final PropertyDescriptor SOURCE_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("source-connection-pool")
            .displayName("Source Connection Pool")
            .required(true)
            .identifiesControllerService(RedisConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor CURSOR_STATE_CACHE = new PropertyDescriptor.Builder()
            .name("cursor-state-cache")
            .displayName("Cursor State Cache")
            .description("Shared with RedisScanReader - used here purely as a hand-off channel to "
                    + "SearchIndexRehydrator, not for any cursor/partition state of this processor's own.")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor MIGRATION_ID = new PropertyDescriptor.Builder()
            .name("migration-id")
            .displayName("Migration ID")
            .description("Namespaces cached definitions so SearchIndexRehydrator (and a target reused "
                    + "across multiple migrations) only ever sees this run's own set.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("batch-timeout-ms")
            .displayName("Batch Timeout (ms)")
            .description("Max time to await the FT.INFO pipeline for all discovered indexes together.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("30000")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
            List.of(SOURCE_CONNECTION_POOL, CURSOR_STATE_CACHE, MIGRATION_ID, BATCH_TIMEOUT_MS);

    // No FlowFiles are ever produced - this processor's only output is the cache entries written
    // in @OnScheduled.
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
        RedisConnectionPoolService sourcePool = context.getProperty(SOURCE_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        DistributedMapCacheClient cache = context.getProperty(CURSOR_STATE_CACHE).asControllerService(DistributedMapCacheClient.class);
        String migrationId = context.getProperty(MIGRATION_ID).getValue();
        long batchTimeoutMs = context.getProperty(BATCH_TIMEOUT_MS).asLong();
        ComponentLog logger = getLogger();

        List<SearchIndexDefinition> definitions;
        try {
            definitions = sourcePool.withRawConnection(conn -> {
                List<byte[]> rawNames;
                try {
                    rawNames = RawModuleCommands.ftList(conn).get(batchTimeoutMs, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    throw new ProcessException("FT._LIST failed", e);
                }

                Map<String, CompletableFuture<List<Object>>> infoFutures = new LinkedHashMap<>();
                for (byte[] rawName : rawNames) {
                    String name = new String(rawName, StandardCharsets.UTF_8);
                    infoFutures.put(name, RawModuleCommands.ftInfo(conn, rawName));
                }
                try {
                    CompletableFuture.allOf(infoFutures.values().toArray(new CompletableFuture[0])).get(batchTimeoutMs, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    // At least one FT.INFO failed or timed out - which one(s) is resolved per-entry below.
                }

                List<SearchIndexDefinition> defs = new ArrayList<>();
                for (Map.Entry<String, CompletableFuture<List<Object>>> entry : infoFutures.entrySet()) {
                    CompletableFuture<List<Object>> future = entry.getValue();
                    if (future.isDone() && !future.isCompletedExceptionally()) {
                        defs.add(SearchIndexDefinition.parse(entry.getKey(), future.join()));
                    } else {
                        logger.warn("FT.INFO failed or timed out for index {} - it will not be migrated", entry.getKey());
                    }
                }
                return defs;
            });
        } catch (Exception e) {
            throw new ProcessException("Search index export failed", e);
        }

        try {
            List<String> names = new ArrayList<>();
            for (SearchIndexDefinition def : definitions) {
                cache.put(SearchIndexCache.definitionKey(migrationId, def.index), def,
                        SearchIndexCache.KEY_SERIALIZER, SearchIndexCache.DEFINITION_SERIALIZER);
                names.add(def.index);
            }
            cache.put(SearchIndexCache.manifestKey(migrationId), names,
                    SearchIndexCache.KEY_SERIALIZER, SearchIndexCache.NAMES_SERIALIZER);
            logger.info("Exported {} search index definition(s) for migration '{}'", names.size(), migrationId);
        } catch (IOException e) {
            throw new ProcessException("Unable to cache search index definitions", e);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        // All real work already happened in @OnScheduled, exactly once - nothing left to do
        // per-trigger, ever.
        context.yield();
    }
}
