package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.DragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.RawModuleCommands;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RestoreArgs;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handles keys RedisScanReader routed to {@code unknown} because their {@code TYPE} reply is a
 * module type, per spec section 8.3. Reads and writes ReJSON-RL and TopK directly (bypassing
 * RedisBatchWriter/CommandBuilder, matching Appendix B's "Via ModuleTypeHandler" annotation on
 * both the JSON.SET and TOPK.RESERVE/ADD write commands); everything else falls back to the
 * failure relationship, wired to RedisDeadLetterWriter in the flow.
 *
 * <p>Processes up to {@link #BATCH_SIZE} FlowFiles per trigger, pipelining every read and every
 * write within a batch as concurrent async commands on a single borrowed connection (mirroring
 * RedisBatchWriter's dispatch-then-await-all pattern) rather than one blocking round trip per
 * key. The original one-key-at-a-time version was fine against a local, near-zero-latency
 * target, but against a real remote endpoint (e.g. DragonflyDB Cloud) two sequential blocking
 * round trips per key throttled ReJSON-RL throughput to roughly the network RTT, ~20-25 keys/s -
 * verified in practice as the actual bottleneck in an otherwise-pipelined flow (RedisBatchWriter
 * already batches/pipelines the core-type path and ran at ~2500 keys/s against the same target).
 *
 * <p>TopK reconstruction restores exact per-item counts via {@code TOPK.RESERVE} (original
 * k/width/depth/decay, from {@code TOPK.INFO}) followed by {@code TOPK.INCRBY} for each member's
 * real recorded count (from {@code TOPK.LIST key WITHCOUNT}), chunked to respect the module's
 * <=100000-per-increment cap, directly against the real key as it flows through this processor
 * (see run-r2dfly.sh's --topk-mode). The exact {@code TYPE} reply string RedisBloom uses for
 * TopK keys was confirmed live against a real Dragonfly instance ({@code TopK-TYPE}), but is
 * still matched loosely (case-insensitive "topk" substring) rather than by exact equality, in
 * case another module version replies differently.
 */
public class ModuleTypeHandler extends AbstractProcessor {

    public static final PropertyDescriptor SOURCE_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("source-connection-pool")
            .displayName("Source Connection Pool")
            .required(true)
            .identifiesControllerService(RedisConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor TARGET_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("target-connection-pool")
            .displayName("Target Connection Pool")
            .required(true)
            .identifiesControllerService(DragonflyConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor KEY_PREFIX = RedisBatchWriter.KEY_PREFIX;
    public static final PropertyDescriptor KEY_PREFIX_SEPARATOR = RedisBatchWriter.KEY_PREFIX_SEPARATOR;

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("Batch Size")
            .description("Default number of FlowFiles of a given module-key category (json/topk/bloom-cms, "
                    + "see the per-category overrides below) to pipeline reads and writes for per trigger, "
                    + "instead of one blocking round trip per key - for any category without its own "
                    + "override. Also the combined cap across all such unoverridden categories together in "
                    + "one trigger, exactly as before this property split into per-category overrides "
                    + "existed.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("50")
            .build();

    private static PropertyDescriptor.Builder perCategoryBatchSizeBuilder(String description, String name) {
        return new PropertyDescriptor.Builder()
                .name("batch-size-" + name)
                .displayName("Batch Size (" + description + ")")
                .description("Overrides Batch Size for " + description + " keys specifically - independent "
                        + "of, and in addition to, Batch Size's own combined cap on every other category. "
                        + "Unset (the default) means these keys are governed by Batch Size like any other "
                        + "category without an override.")
                .required(false)
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR);
    }

    public static final PropertyDescriptor BATCH_SIZE_JSON = perCategoryBatchSizeBuilder("ReJSON-RL", "json").build();
    public static final PropertyDescriptor BATCH_SIZE_TOPK = perCategoryBatchSizeBuilder("TopK (approximate-mode reconstruction)", "topk").build();
    public static final PropertyDescriptor BATCH_SIZE_BLOOM_CMS = perCategoryBatchSizeBuilder(
            "Bloom/Cuckoo Filter/CMS (--dfly-to-dfly DUMP/RESTORE-only keys)", "bloom-cms").build();

    public static final PropertyDescriptor BATCH_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("batch-timeout-ms")
            .displayName("Batch Timeout (ms)")
            .description("Max time to await pipeline completion for one batch's reads, or one batch's writes.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();

    public static final PropertyDescriptor DFLY_TO_DFLY = new PropertyDescriptor.Builder()
            .name("dfly-to-dfly")
            .displayName("Dragonfly-to-Dragonfly Optimizations")
            .description("Enable when both source and target are Dragonfly. Uses DUMP/RESTORE - a "
                    + "byte-for-byte copy of the key's internal serialization - instead of type-specific "
                    + "read/reconstruct commands, for ReJSON-RL, TopK-TYPE, MBbloom-- (Bloom filter), "
                    + "MBbloomCF (Cuckoo Filter), and CMSk-TYPE (Count-Min Sketch) keys. Roughly halves "
                    + "round trips (one DUMP+RESTORE pair instead of a read command followed by a rebuild "
                    + "command) and, for TopK, is exact rather than the approximate TOPK.ADD-based "
                    + "reconstruction. Bloom, Cuckoo Filter, and CMS keys have no other reconstruction "
                    + "path in this processor, so they're only migrated when this is enabled; ReJSON-RL "
                    + "and TopK-TYPE fall back to their normal reconstruction for any key where DUMP or "
                    + "RESTORE itself fails. Only safe between Dragonfly instances on compatible versions "
                    + "- DUMP payloads aren't a portable format, and Cuckoo Filter support itself is "
                    + "version-gated on Dragonfly's side (e.g. present in df-v1.40.2, absent in "
                    + "df-v1.39.0).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Module key reconstructed on the target").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Unsupported module type or reconstruction failed").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SOURCE_CONNECTION_POOL, TARGET_CONNECTION_POOL, KEY_PREFIX, KEY_PREFIX_SEPARATOR, BATCH_SIZE,
            BATCH_SIZE_JSON, BATCH_SIZE_TOPK, BATCH_SIZE_BLOOM_CMS, BATCH_TIMEOUT_MS, DFLY_TO_DFLY);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /** One FlowFile's worth of module-key context, carried through the batched read/write phases. */
    private record Item(FlowFile flowFile, byte[] sourceKey, byte[] targetKey, long ttlMs) {
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        long batchTimeoutMs = context.getProperty(BATCH_TIMEOUT_MS).asLong();
        // Read here (not down with the other properties below) because the batch pull right
        // below needs it - ModuleBatchFilter's own category classification (json/topk/
        // bloom-cms/default) has to mirror the exact same dflyToDfly-gated logic the per-item
        // loop below uses, or the two would disagree about which category a key belongs to.
        boolean dflyToDflyForBatching = context.getProperty(DFLY_TO_DFLY).asBoolean();
        Map<String, Integer> categoryBatchSize = new HashMap<>();
        putIfSet(categoryBatchSize, "json", context, BATCH_SIZE_JSON);
        putIfSet(categoryBatchSize, "topk", context, BATCH_SIZE_TOPK);
        putIfSet(categoryBatchSize, "bloomcms", context, BATCH_SIZE_BLOOM_CMS);
        List<FlowFile> batch = session.get(new ModuleBatchFilter(batchSize, categoryBatchSize, dflyToDflyForBatching));
        if (batch.isEmpty()) {
            context.yield();
            return;
        }

        RedisConnectionPoolService sourcePool = context.getProperty(SOURCE_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        DragonflyConnectionPoolService targetPool = context.getProperty(TARGET_CONNECTION_POOL).asControllerService(DragonflyConnectionPoolService.class);
        String prefix = context.getProperty(KEY_PREFIX).getValue();
        String separator = context.getProperty(KEY_PREFIX_SEPARATOR).getValue();
        boolean dflyToDfly = dflyToDflyForBatching;
        ComponentLog logger = getLogger();

        List<Item> jsonItems = new ArrayList<>();
        List<Item> topkItems = new ArrayList<>();
        List<Item> dumpRestoreOnlyItems = new ArrayList<>();

        for (FlowFile flowFile : batch) {
            String key = flowFile.getAttribute("redis.key");
            String moduleType = flowFile.getAttribute("redis.type");
            long ttlMs = parseLong(flowFile.getAttribute("redis.ttl.ms"), -1);
            if (key == null || moduleType == null) {
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                continue;
            }
            byte[] sourceKey = key.getBytes(StandardCharsets.UTF_8);
            byte[] targetKey = ((prefix == null || prefix.isEmpty()) ? key : prefix + separator + key).getBytes(StandardCharsets.UTF_8);
            Item item = new Item(flowFile, sourceKey, targetKey, ttlMs);
            switch (classifyForBatch(moduleType, dflyToDfly)) {
                case "json" -> jsonItems.add(item);
                case "topk" -> topkItems.add(item);
                case "bloomcms" ->
                    // No read/reconstruct command exists for these in this processor - DUMP/RESTORE
                    // is the only way they're migrated at all, so they're only reachable here.
                        dumpRestoreOnlyItems.add(item);
                default -> {
                    FlowFile updated = session.putAttribute(flowFile, "redis.incompatible.reason", "module_type:" + moduleType);
                    logger.warn("Unsupported module type '{}' for key {}", moduleType, key);
                    session.transfer(updated, REL_FAILURE);
                }
            }
        }

        if (!jsonItems.isEmpty()) {
            reconstructJsonBatch(sourcePool, targetPool, jsonItems, batchTimeoutMs, session, logger, dflyToDfly);
        }
        if (!topkItems.isEmpty()) {
            reconstructTopKBatch(sourcePool, targetPool, topkItems, batchTimeoutMs, session, logger, dflyToDfly);
        }
        if (!dumpRestoreOnlyItems.isEmpty()) {
            List<Item> failed = dumpRestoreBatch(sourcePool, targetPool, dumpRestoreOnlyItems, batchTimeoutMs, session, logger);
            for (Item item : failed) {
                FlowFile updated = session.putAttribute(item.flowFile, "redis.incompatible.reason", "dump_restore_failed");
                session.transfer(session.penalize(updated), REL_FAILURE);
            }
        }
        session.commit();
    }

    private void reconstructJsonBatch(RedisConnectionPoolService sourcePool, DragonflyConnectionPoolService targetPool,
                                       List<Item> inputItems, long batchTimeoutMs, ProcessSession session, ComponentLog logger,
                                       boolean dflyToDfly) {
        List<Item> items = dflyToDfly
                ? dumpRestoreBatch(sourcePool, targetPool, inputItems, batchTimeoutMs, session, logger)
                : inputItems;
        if (items.isEmpty()) {
            return;
        }
        Map<Item, CompletableFuture<byte[]>> getFutures = sourcePool.withRawConnection(conn -> {
            Map<Item, CompletableFuture<byte[]>> futures = new LinkedHashMap<>();
            for (Item item : items) {
                futures.put(item, RawModuleCommands.jsonGet(conn, item.sourceKey));
            }
            awaitAll(futures.values(), batchTimeoutMs);
            return futures;
        });

        List<Item> toWrite = new ArrayList<>();
        Map<Item, byte[]> jsonByItem = new LinkedHashMap<>();
        for (Item item : items) {
            byte[] json = resolveOrLogFailure(getFutures.get(item), item, "JSON.GET", session, logger);
            if (json == null) {
                continue;
            }
            jsonByItem.put(item, json);
            toWrite.add(item);
        }
        if (toWrite.isEmpty()) {
            return;
        }

        Map<Item, CompletableFuture<String>> setFutures = targetPool.withRawConnection(conn -> {
            Map<Item, CompletableFuture<String>> futures = new LinkedHashMap<>();
            for (Item item : toWrite) {
                futures.put(item, RawModuleCommands.jsonSet(conn, item.targetKey, jsonByItem.get(item)));
            }
            awaitAll(futures.values(), batchTimeoutMs);
            return futures;
        });

        List<Item> succeeded = new ArrayList<>();
        for (Item item : toWrite) {
            if (resolveOrLogFailure(setFutures.get(item), item, "JSON.SET", session, logger) != null) {
                succeeded.add(item);
                session.transfer(item.flowFile, REL_SUCCESS);
            }
        }
        applyTtlBatch(targetPool, succeeded, batchTimeoutMs, logger);
    }

    /** RedisBloom/Dragonfly reject a single TOPK.INCRBY increment over 100000 - found live
     * against production-scale data. */
    private static final long MAX_TOPK_INCR = 100_000L;
    /** Defensive cap on item/increment pairs per TOPK.INCRBY call, so a key with an unusually
     * large tracked-item set can't produce one unbounded command. */
    private static final int TOPK_INCRBY_BATCH_LIMIT = 900;

    private void reconstructTopKBatch(RedisConnectionPoolService sourcePool, DragonflyConnectionPoolService targetPool,
                                       List<Item> inputItems, long batchTimeoutMs, ProcessSession session, ComponentLog logger,
                                       boolean dflyToDfly) {
        // The DUMP/RESTORE fast path below also gives exact counts (verified directly against
        // two real Dragonfly instances) - the TOPK.INCRBY path further down is exact too now, so
        // either way this method's output is exact, not approximate.
        List<Item> items = dflyToDfly
                ? dumpRestoreBatch(sourcePool, targetPool, inputItems, batchTimeoutMs, session, logger)
                : inputItems;
        if (items.isEmpty()) {
            return;
        }
        Map<Item, CompletableFuture<Map<String, String>>> infoFutures = sourcePool.withRawConnection(conn -> {
            Map<Item, CompletableFuture<Map<String, String>>> futures = new LinkedHashMap<>();
            for (Item item : items) {
                futures.put(item, RawModuleCommands.topkInfo(conn, item.sourceKey));
            }
            awaitAll(futures.values(), batchTimeoutMs);
            return futures;
        });
        Map<Item, CompletableFuture<List<Map.Entry<String, Long>>>> listFutures = sourcePool.withRawConnection(conn -> {
            Map<Item, CompletableFuture<List<Map.Entry<String, Long>>>> futures = new LinkedHashMap<>();
            for (Item item : items) {
                futures.put(item, RawModuleCommands.topkListWithCount(conn, item.sourceKey));
            }
            awaitAll(futures.values(), batchTimeoutMs);
            return futures;
        });

        List<Item> toWrite = new ArrayList<>();
        Map<Item, Map<String, String>> infoByItem = new LinkedHashMap<>();
        Map<Item, List<Map.Entry<String, Long>>> listByItem = new LinkedHashMap<>();
        for (Item item : items) {
            Map<String, String> info = resolveOrLogFailure(infoFutures.get(item), item, "TOPK.INFO", session, logger);
            if (info == null) {
                continue;
            }
            List<Map.Entry<String, Long>> list = resolveOrLogFailure(listFutures.get(item), item, "TOPK.LIST", session, logger);
            if (list == null) {
                continue;
            }
            infoByItem.put(item, info);
            listByItem.put(item, list);
            toWrite.add(item);
        }
        if (toWrite.isEmpty()) {
            return;
        }

        Map<Item, CompletableFuture<String>> reserveFutures = targetPool.withRawConnection(conn -> {
            Map<Item, CompletableFuture<String>> futures = new LinkedHashMap<>();
            // TOPK.RESERVE errors on an already-existing key, so a per-item DEL must land on the
            // wire first - dispatched inside the same loop iteration so Lettuce's call-order wire
            // ordering keeps DEL ahead of RESERVE for that key.
            List<CompletableFuture<?>> delFutures = new ArrayList<>();
            for (Item item : toWrite) {
                delFutures.add(RawModuleCommands.del(conn, item.targetKey));
                Map<String, String> info = infoByItem.get(item);
                long k = Long.parseLong(info.getOrDefault("k", "50"));
                long width = Long.parseLong(info.getOrDefault("width", "8"));
                long depth = Long.parseLong(info.getOrDefault("depth", "7"));
                double decay = Double.parseDouble(info.getOrDefault("decay", "0.9"));
                futures.put(item, RawModuleCommands.topkReserve(conn, item.targetKey, k, width, depth, decay));
            }
            List<CompletableFuture<?>> allDispatched = new ArrayList<>(delFutures);
            allDispatched.addAll(futures.values());
            awaitAll(allDispatched, batchTimeoutMs);
            return futures;
        });

        List<Item> reserved = new ArrayList<>();
        for (Item item : toWrite) {
            if (resolveOrLogFailure(reserveFutures.get(item), item, "TOPK.RESERVE", session, logger) != null) {
                reserved.add(item);
            }
        }
        if (reserved.isEmpty()) {
            return;
        }

        List<Item> nonEmpty = reserved.stream().filter(item -> !listByItem.get(item).isEmpty()).toList();
        // One item's exact counts can need more than one TOPK.INCRBY call (chunkForIncrBy splits
        // on both the 100000-per-increment cap and TOPK_INCRBY_BATCH_LIMIT), so every batch for
        // every item is dispatched concurrently here, then resolved back per-item below - a
        // FlowFile can only be transferred once per session, so success/failure has to be decided
        // once per item, not once per batch.
        List<IncrByBatch> incrByBatches = new ArrayList<>();
        for (Item item : nonEmpty) {
            List<List<Map.Entry<byte[], Long>>> chunks = chunkForIncrBy(listByItem.get(item));
            for (int i = 0; i < chunks.size(); i++) {
                incrByBatches.add(new IncrByBatch(item, i, chunks.get(i)));
            }
        }
        Map<IncrByBatch, CompletableFuture<String>> incrByFutures = targetPool.withRawConnection(conn -> {
            Map<IncrByBatch, CompletableFuture<String>> futures = new LinkedHashMap<>();
            for (IncrByBatch batch : incrByBatches) {
                futures.put(batch, RawModuleCommands.topkIncrBy(conn, batch.item.targetKey, batch.pairs));
            }
            awaitAll(futures.values(), batchTimeoutMs);
            return futures;
        });

        Set<Item> failedItems = new HashSet<>();
        for (IncrByBatch batch : incrByBatches) {
            CompletableFuture<String> future = incrByFutures.get(batch);
            if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
                continue;
            }
            failedItems.add(batch.item);
            Throwable cause = null;
            if (future != null && future.isCompletedExceptionally()) {
                try {
                    future.join();
                } catch (Exception e) {
                    cause = e.getCause() != null ? e.getCause() : e;
                }
            }
            logger.warn("TOPK.INCRBY batch {} failed for key {}", batch.index, keyString(batch.item), cause);
        }

        List<Item> succeeded = new ArrayList<>();
        for (Item item : reserved) {
            if (nonEmpty.contains(item) && failedItems.contains(item)) {
                session.transfer(session.penalize(item.flowFile), REL_FAILURE);
                continue;
            }
            succeeded.add(item);
            session.transfer(item.flowFile, REL_SUCCESS);
        }
        applyTtlBatch(targetPool, succeeded, batchTimeoutMs, logger);
    }

    /** One TOPK.INCRBY call's worth of item/increment pairs for one item's reconstruction -
     * {@code index} is only for log messages, distinguishing which of an item's (possibly
     * several) chunks failed. */
    private record IncrByBatch(Item item, int index, List<Map.Entry<byte[], Long>> pairs) {
    }

    /** Splits one item's {member -> exact count} pairs into one or more TOPK.INCRBY argument
     * groups, respecting Dragonfly/RedisBloom's <=100000-per-increment cap (a single member's
     * count over that must be issued as multiple increments for the same member) and
     * TOPK_INCRBY_BATCH_LIMIT pairs per call. */
    private static List<List<Map.Entry<byte[], Long>>> chunkForIncrBy(List<Map.Entry<String, Long>> counts) {
        List<List<Map.Entry<byte[], Long>>> batches = new ArrayList<>();
        List<Map.Entry<byte[], Long>> current = new ArrayList<>();
        for (Map.Entry<String, Long> entry : counts) {
            byte[] member = entry.getKey().getBytes(StandardCharsets.UTF_8);
            long remaining = entry.getValue();
            while (remaining > 0) {
                long chunk = Math.min(remaining, MAX_TOPK_INCR);
                current.add(Map.entry(member, chunk));
                remaining -= chunk;
                if (current.size() >= TOPK_INCRBY_BATCH_LIMIT) {
                    batches.add(current);
                    current = new ArrayList<>();
                }
            }
        }
        if (!current.isEmpty()) {
            batches.add(current);
        }
        return batches;
    }

    /** DUMP/RESTORE fast path for --dfly-to-dfly: a byte-for-byte copy of the key's internal
     * serialization (one DUMP+RESTORE pair) instead of a type-specific read/rebuild round trip.
     * TTL rides along in the RESTORE call itself, so no separate PEXPIRE is needed for items that
     * succeed here. Returns the items that failed (DUMP or RESTORE error, or the key vanished
     * between scan and now) - the caller decides what to do with them: fall back to a
     * type-specific reconstruction (JSON, TopK), or route straight to failure (Bloom, CMS, which
     * have no other reconstruction path in this processor). */
    private List<Item> dumpRestoreBatch(RedisConnectionPoolService sourcePool, DragonflyConnectionPoolService targetPool,
                                         List<Item> items, long batchTimeoutMs, ProcessSession session, ComponentLog logger) {
        Map<Item, RedisFuture<byte[]>> dumpFutures = sourcePool.withConnection(cmds -> {
            Map<Item, RedisFuture<byte[]>> futures = new LinkedHashMap<>();
            for (Item item : items) {
                futures.put(item, cmds.dump(item.sourceKey));
            }
            awaitAll(futures.values().toArray(new RedisFuture[0]), batchTimeoutMs);
            return futures;
        });

        List<Item> failed = new ArrayList<>();
        List<Item> toRestore = new ArrayList<>();
        Map<Item, byte[]> payloadByItem = new LinkedHashMap<>();
        for (Item item : items) {
            RedisFuture<byte[]> future = dumpFutures.get(item);
            byte[] payload = null;
            if (future != null && future.isDone() && future.getError() == null) {
                try {
                    payload = future.get();
                } catch (Exception ignored) {
                    // treated as failure below
                }
            }
            if (payload == null) {
                logger.warn("DUMP failed for key {}: {}", keyString(item), future == null ? "no future" : future.getError());
                failed.add(item);
                continue;
            }
            payloadByItem.put(item, payload);
            toRestore.add(item);
        }
        if (toRestore.isEmpty()) {
            return failed;
        }

        Map<Item, RedisFuture<String>> restoreFutures = targetPool.withConnection(cmds -> {
            Map<Item, RedisFuture<String>> futures = new LinkedHashMap<>();
            for (Item item : toRestore) {
                RestoreArgs args = new RestoreArgs().replace().ttl(item.ttlMs > 0 ? item.ttlMs : 0);
                futures.put(item, cmds.restore(item.targetKey, payloadByItem.get(item), args));
            }
            awaitAll(futures.values().toArray(new RedisFuture[0]), batchTimeoutMs);
            return futures;
        });

        for (Item item : toRestore) {
            RedisFuture<String> future = restoreFutures.get(item);
            if (future != null && future.isDone() && future.getError() == null) {
                session.transfer(item.flowFile, REL_SUCCESS);
            } else {
                logger.warn("RESTORE failed for key {}: {}", keyString(item), future == null ? "no future" : future.getError());
                failed.add(item);
            }
        }
        return failed;
    }

    private static String keyString(Item item) {
        return new String(item.sourceKey, StandardCharsets.UTF_8);
    }

    /** Pipelines a PEXPIRE per item with a TTL across one borrowed connection; best-effort - a
     * TTL failure is logged but doesn't undo the already-successful key migration. */
    private void applyTtlBatch(DragonflyConnectionPoolService targetPool, List<Item> items, long batchTimeoutMs, ComponentLog logger) {
        List<Item> withTtl = items.stream().filter(item -> item.ttlMs > 0).toList();
        if (withTtl.isEmpty()) {
            return;
        }
        Map<Item, RedisFuture<Boolean>> futures = targetPool.withConnection(cmds -> {
            Map<Item, RedisFuture<Boolean>> f = new LinkedHashMap<>();
            for (Item item : withTtl) {
                f.put(item, cmds.pexpire(item.targetKey, item.ttlMs));
            }
            awaitAll(f.values().toArray(new RedisFuture[0]), batchTimeoutMs);
            return f;
        });
        for (Map.Entry<Item, RedisFuture<Boolean>> entry : futures.entrySet()) {
            if (!entry.getValue().isDone() || entry.getValue().getError() != null) {
                logger.warn("Failed to apply TTL for key {}", new String(entry.getKey().targetKey, StandardCharsets.UTF_8));
            }
        }
    }

    /** Resolves a completed future's value, logging and returning null (never throwing) if it
     * failed or didn't finish in time - the caller treats null as "skip this item". */
    private <T> T resolveOrLogFailure(CompletableFuture<T> future, Item item, String commandName,
                                       ProcessSession session, ComponentLog logger) {
        if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
            return future.join();
        }
        String key = new String(item.sourceKey, StandardCharsets.UTF_8);
        if (future == null) {
            logger.warn("{} produced no future for key {}", commandName, key);
        } else if (future.isCompletedExceptionally()) {
            Throwable cause = null;
            try {
                future.join();
            } catch (Exception e) {
                cause = e.getCause() != null ? e.getCause() : e;
            }
            logger.warn("{} failed for key {}", commandName, key, cause);
        } else {
            logger.warn("{} did not complete in time for key {}", commandName, key);
        }
        session.transfer(session.penalize(item.flowFile), REL_FAILURE);
        return null;
    }

    private static void awaitAll(Collection<? extends CompletableFuture<?>> futures, long timeoutMs) {
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // At least one future in this pipeline failed or timed out - which one(s) is
            // resolved per-future by the caller via resolveOrLogFailure, not here.
        }
    }

    // LettuceFutures.awaitAll (unlike CompletableFuture.allOf(...).get above) throws if any
    // future completed exceptionally, which would otherwise abort the whole DUMP/RESTORE/TTL
    // batch instead of letting each item's failure be handled individually by its caller.
    private static void awaitAll(RedisFuture<?>[] futures, long timeoutMs) {
        try {
            LettuceFutures.awaitAll(Duration.ofMillis(timeoutMs), futures);
        } catch (Exception ignored) {
            // resolved (and any failure handled) per-future below
        }
    }

    private static long parseLong(String value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static void putIfSet(Map<String, Integer> map, String category, ProcessContext context, PropertyDescriptor descriptor) {
        if (context.getProperty(descriptor).isSet()) {
            map.put(category, context.getProperty(descriptor).asInteger());
        }
    }

    /** Classifies a redis.type value into the same buckets onTrigger's own dispatch loop routes
     * on ("json", "topk", "bloomcms", or "other" for anything unsupported) - shared by
     * ModuleBatchFilter below so the batch pull can never disagree with the dispatch loop about
     * which category a key belongs to. Mirrors that loop's matching exactly: ReJSON-RL by exact
     * case-insensitive name, TopK by a loose "topk" substring (the real TYPE reply for RedisBloom
     * TopK keys couldn't be confirmed without a live server), Bloom/Cuckoo Filter/CMS only when
     * dflyToDfly is on (they have no other reconstruction path in this processor). */
    private static String classifyForBatch(String moduleType, boolean dflyToDfly) {
        if (moduleType == null) {
            return "other";
        }
        String lower = moduleType.toLowerCase();
        if ("rejson-rl".equals(lower)) {
            return "json";
        }
        if (lower.contains("topk")) {
            return "topk";
        }
        if (dflyToDfly && (lower.contains("bloom") || lower.contains("cms"))) {
            return "bloomcms";
        }
        return "other";
    }

    // ModuleBatchFilter buckets by classifyForBatch's category: a category with its own
    // categoryBatchSize entry gets an independent cap, fully separate from every other category;
    // every category WITHOUT an entry (including "other", the unsupported/immediate-failure
    // case) shares one combined "__default__" bucket capped at defaultBatchSize - exactly the
    // single global cap this processor's plain session.get(batchSize) enforced before per-
    // category overrides existed, so an unconfigured migration (categoryBatchSize empty) behaves
    // byte-for-byte the same as before. See RedisBatchWriter's ReadyBatchFilter for the identical
    // pattern (including the scanned-count safety valve) - not shared as one class between the
    // two processors since their category classification logic itself differs.
    private static final class ModuleBatchFilter implements FlowFileFilter {
        private static final int MAX_SCAN_MULTIPLIER = 20;
        private static final String DEFAULT_BUCKET = "__default__";

        private final int defaultBatchSize;
        private final Map<String, Integer> categoryLimits;
        private final boolean dflyToDfly;
        private final Map<String, Integer> acceptedByBucket = new HashMap<>();
        private int scanned = 0;

        ModuleBatchFilter(int defaultBatchSize, Map<String, Integer> categoryLimits, boolean dflyToDfly) {
            this.defaultBatchSize = defaultBatchSize;
            this.categoryLimits = categoryLimits;
            this.dflyToDfly = dflyToDfly;
        }

        @Override
        public FlowFileFilterResult filter(FlowFile flowFile) {
            scanned++;
            String category = classifyForBatch(flowFile.getAttribute("redis.type"), dflyToDfly);
            Integer overrideLimit = categoryLimits.get(category);
            String bucket = overrideLimit != null ? category : DEFAULT_BUCKET;
            int limit = overrideLimit != null ? overrideLimit : defaultBatchSize;
            int acceptedSoFar = acceptedByBucket.getOrDefault(bucket, 0);
            if (acceptedSoFar >= limit) {
                if (categoryLimits.isEmpty() || scanned >= (long) defaultBatchSize * MAX_SCAN_MULTIPLIER) {
                    return FlowFileFilterResult.REJECT_AND_TERMINATE;
                }
                return FlowFileFilterResult.REJECT_AND_CONTINUE;
            }
            acceptedByBucket.put(bucket, acceptedSoFar + 1);
            return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
        }
    }
}
