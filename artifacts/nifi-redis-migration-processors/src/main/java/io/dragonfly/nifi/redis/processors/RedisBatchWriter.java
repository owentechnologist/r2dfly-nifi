package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.DragonflyConnectionPoolService;
import io.dragonfly.nifi.redis.util.ChunkReassembler;
import io.dragonfly.nifi.redis.util.CommandBuilder;
import io.dragonfly.nifi.redis.util.KeyRecord;
import io.dragonfly.nifi.redis.util.RedisTypeSerializer;
import io.dragonfly.nifi.redis.util.RetryPolicy;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Tags({"redis", "dragonfly", "migration", "sink", "target"})
@CapabilityDescription("Accepts FlowFiles from RedisTypeDeserializer/RedisSingleKeyFetch, batches them, and "
        + "issues pipelined write commands to Dragonfly. Spec section 4.3.")
public class RedisBatchWriter extends AbstractProcessor {

    public static final PropertyDescriptor DRAGONFLY_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("dragonfly-connection-pool")
            .displayName("Dragonfly Connection Pool")
            .required(true)
            .identifiesControllerService(DragonflyConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("Batch Size")
            .description("Default number of FlowFiles of a given Redis type to pipeline per trigger, for "
                    + "any type without its own override below. Also the combined cap across all such "
                    + "unoverridden types together in one trigger, exactly as before this property split "
                    + "into per-type overrides existed.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("50")
            .build();

    private static PropertyDescriptor.Builder perTypeBatchSizeBuilder(String type, String name) {
        return new PropertyDescriptor.Builder()
                .name("batch-size-" + name)
                .displayName("Batch Size (" + type + ")")
                .description("Overrides Batch Size for " + type + " keys specifically - independent of, "
                        + "and in addition to, Batch Size's own combined cap on every other type. Unset "
                        + "(the default) means " + type + " keys are governed by Batch Size like any other "
                        + "type without an override.")
                .required(false)
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR);
    }

    public static final PropertyDescriptor BATCH_SIZE_STRING = perTypeBatchSizeBuilder("string", "string").build();
    public static final PropertyDescriptor BATCH_SIZE_HASH = perTypeBatchSizeBuilder("hash", "hash").build();
    public static final PropertyDescriptor BATCH_SIZE_LIST = perTypeBatchSizeBuilder("list", "list").build();
    public static final PropertyDescriptor BATCH_SIZE_SET = perTypeBatchSizeBuilder("set", "set").build();
    public static final PropertyDescriptor BATCH_SIZE_ZSET = perTypeBatchSizeBuilder("sorted set (zset)", "zset").build();
    public static final PropertyDescriptor BATCH_SIZE_STREAM = perTypeBatchSizeBuilder("stream", "stream").build();

    public static final PropertyDescriptor WRITE_CHUNK_SIZE = new PropertyDescriptor.Builder()
            .name("write-chunk-size")
            .displayName("Write Chunk Size")
            .description("Max elements/fields/members sent to Dragonfly in a single RPUSH/SADD/ZADD/HSET "
                    + "call when writing one key. A key with more entries than this is written across "
                    + "several smaller commands instead of one command whose argument count scales with "
                    + "the key's own size (e.g. a multi-million-member set becoming one giant SADD).")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue(String.valueOf(CommandBuilder.DEFAULT_WRITE_CHUNK_SIZE))
            .build();

    public static final PropertyDescriptor BATCH_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("batch-timeout-ms")
            .displayName("Batch Timeout (ms)")
            .description("Max time to await pipeline completion for one batch flush. 30s default "
                    + "accounts for large aggregate keys (e.g. multi-million-member sets/zsets) "
                    + "taking real time to transmit even when chunked/pipelined - a short timeout "
                    + "here just routes the write to retry while the original attempt keeps running "
                    + "in the background, risking overlapping writes to the same key.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("30000")
            .build();

    public static final PropertyDescriptor MAX_PIPELINE_DEPTH = new PropertyDescriptor.Builder()
            .name("max-pipeline-depth")
            .displayName("Max Pipeline Depth")
            .description("Max outstanding key writes dispatched concurrently before awaiting completions.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("2000")
            .build();

    public static final PropertyDescriptor TTL_STRATEGY = new PropertyDescriptor.Builder()
            .name("ttl-strategy")
            .displayName("TTL Strategy")
            .required(true)
            .allowableValues(CommandBuilder.TtlStrategy.PRESERVE.name(), CommandBuilder.TtlStrategy.STRIP.name(), CommandBuilder.TtlStrategy.RESET.name())
            .defaultValue(CommandBuilder.TtlStrategy.PRESERVE.name())
            .build();

    public static final PropertyDescriptor TTL_RESET_OFFSET_MS = new PropertyDescriptor.Builder()
            .name("ttl-reset-offset-ms")
            .displayName("TTL Reset Offset (ms)")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();

    public static final PropertyDescriptor KEY_PREFIX = new PropertyDescriptor.Builder()
            .name("key-prefix")
            .displayName("Key Prefix")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_PREFIX_SEPARATOR = new PropertyDescriptor.Builder()
            .name("key-prefix-separator")
            .displayName("Key Prefix Separator")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(":")
            .build();

    public static final PropertyDescriptor CONFLICT_STRATEGY = new PropertyDescriptor.Builder()
            .name("conflict-strategy")
            .displayName("Conflict Strategy")
            .required(true)
            .allowableValues(CommandBuilder.ConflictStrategy.OVERWRITE.name(), CommandBuilder.ConflictStrategy.SKIP.name(), CommandBuilder.ConflictStrategy.FAIL.name())
            .defaultValue(CommandBuilder.ConflictStrategy.OVERWRITE.name())
            .build();

    public static final PropertyDescriptor WRITE_CONFIRMATION = new PropertyDescriptor.Builder()
            .name("write-confirmation")
            .displayName("Write Confirmation")
            .description("Await pipeline completion before committing the session. Disabling this means "
                    + "FlowFiles are routed to success without confirmation that Dragonfly accepted the "
                    + "write - the spec's own rationale (never fire-and-forget) argues against disabling it.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Key written to Dragonfly successfully").build();
    public static final Relationship REL_SKIPPED = new Relationship.Builder().name("skipped").description("Key skipped per conflict strategy").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Write failed").build();
    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry").description("Transient failure; route back to this processor's incoming queue").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            DRAGONFLY_CONNECTION_POOL, BATCH_SIZE, BATCH_SIZE_STRING, BATCH_SIZE_HASH, BATCH_SIZE_LIST,
            BATCH_SIZE_SET, BATCH_SIZE_ZSET, BATCH_SIZE_STREAM, WRITE_CHUNK_SIZE, BATCH_TIMEOUT_MS,
            MAX_PIPELINE_DEPTH, TTL_STRATEGY, TTL_RESET_OFFSET_MS, KEY_PREFIX, KEY_PREFIX_SEPARATOR,
            CONFLICT_STRATEGY, WRITE_CONFIRMATION);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_SKIPPED, REL_FAILURE, REL_RETRY);

    private final ChunkReassembler chunkReassembler = new ChunkReassembler();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        Map<String, Integer> perTypeBatchSize = new HashMap<>();
        putIfSet(perTypeBatchSize, "string", context, BATCH_SIZE_STRING);
        putIfSet(perTypeBatchSize, "hash", context, BATCH_SIZE_HASH);
        putIfSet(perTypeBatchSize, "list", context, BATCH_SIZE_LIST);
        putIfSet(perTypeBatchSize, "set", context, BATCH_SIZE_SET);
        putIfSet(perTypeBatchSize, "zset", context, BATCH_SIZE_ZSET);
        putIfSet(perTypeBatchSize, "stream", context, BATCH_SIZE_STREAM);
        int writeChunkSize = context.getProperty(WRITE_CHUNK_SIZE).asInteger();
        long batchTimeoutMs = context.getProperty(BATCH_TIMEOUT_MS).asLong();
        int maxPipelineDepth = context.getProperty(MAX_PIPELINE_DEPTH).asInteger();
        String keyPrefix = context.getProperty(KEY_PREFIX).getValue();
        String keyPrefixSeparator = context.getProperty(KEY_PREFIX_SEPARATOR).getValue();
        CommandBuilder.TtlStrategy ttlStrategy = CommandBuilder.TtlStrategy.valueOf(context.getProperty(TTL_STRATEGY).getValue());
        long ttlResetOffsetMs = context.getProperty(TTL_RESET_OFFSET_MS).isSet() ? context.getProperty(TTL_RESET_OFFSET_MS).asLong() : 0L;
        CommandBuilder.ConflictStrategy conflictStrategy = CommandBuilder.ConflictStrategy.valueOf(context.getProperty(CONFLICT_STRATEGY).getValue());
        boolean writeConfirmation = context.getProperty(WRITE_CONFIRMATION).asBoolean();
        DragonflyConnectionPoolService dragonflyPool = context.getProperty(DRAGONFLY_CONNECTION_POOL).asControllerService(DragonflyConnectionPoolService.class);
        RetryPolicy retryPolicy = RetryPolicy.standard();

        long now = System.currentTimeMillis();
        List<FlowFile> batch = session.get(new ReadyBatchFilter(batchSize, perTypeBatchSize, now));
        if (batch.isEmpty()) {
            context.yield();
            return;
        }

        List<PendingWrite> ready = new ArrayList<>();
        for (FlowFile flowFile : batch) {
            if ("true".equals(flowFile.getAttribute("redis.dfly-dump"))) {
                byte[] payload;
                try (InputStream in = session.read(flowFile)) {
                    payload = in.readAllBytes();
                } catch (Exception e) {
                    getLogger().error("Failed to read DUMP payload from FlowFile", e);
                    session.transfer(session.penalize(flowFile), REL_FAILURE);
                    continue;
                }
                KeyRecord dumpRecord = new KeyRecord(
                        flowFile.getAttribute("redis.key"), flowFile.getAttribute("redis.type"),
                        parseLong(flowFile.getAttribute("redis.ttl.ms"), -1), null, null);
                dumpRecord.dumpPayload = payload;
                ready.add(new PendingWrite(flowFile, dumpRecord));
                continue;
            }

            KeyRecord record;
            try (InputStream in = session.read(flowFile)) {
                record = RedisTypeSerializer.readJson(in);
            } catch (Exception e) {
                getLogger().error("Failed to parse key record from FlowFile", e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                continue;
            }

            String chunkIndexAttr = flowFile.getAttribute("redis.chunk.index");
            String chunkTotalAttr = flowFile.getAttribute("redis.chunk.total");
            if (chunkIndexAttr != null && chunkTotalAttr != null) {
                int chunkIndex = Integer.parseInt(chunkIndexAttr);
                int chunkTotal = Integer.parseInt(chunkTotalAttr);
                Optional<KeyRecord> merged = chunkReassembler.accumulate(record.key, record, chunkIndex, chunkTotal);
                session.remove(flowFile);
                merged.ifPresent(mergedRecord -> ready.add(new PendingWrite(session.create(), mergedRecord)));
            } else {
                ready.add(new PendingWrite(flowFile, record));
            }
        }

        if (ready.isEmpty()) {
            session.commit();
            return;
        }

        List<CommandBuilder.WriteOutcome> outcomes;
        if (writeConfirmation) {
            outcomes = new ArrayList<>(ready.size());
            for (int start = 0; start < ready.size(); start += maxPipelineDepth) {
                List<PendingWrite> group = ready.subList(start, Math.min(start + maxPipelineDepth, ready.size()));
                outcomes.addAll(dragonflyPool.withConnectionAndRaw((cmds, rawConn) -> dispatchAndAwait(
                        cmds, rawConn, group, keyPrefix, keyPrefixSeparator, ttlStrategy, ttlResetOffsetMs, conflictStrategy, writeChunkSize, batchTimeoutMs, getLogger())));
            }
        } else {
            dragonflyPool.withConnectionAndRaw((cmds, rawConn) -> {
                for (PendingWrite pending : ready) {
                    CommandBuilder.write(cmds, rawConn, pending.record, keyPrefix, keyPrefixSeparator, ttlStrategy, ttlResetOffsetMs, conflictStrategy, writeChunkSize);
                }
                return null;
            });
            outcomes = new ArrayList<>();
            for (int i = 0; i < ready.size(); i++) {
                outcomes.add(CommandBuilder.WriteOutcome.SUCCESS);
            }
        }

        for (int i = 0; i < ready.size(); i++) {
            FlowFile flowFile = ready.get(i).flowFile;
            CommandBuilder.WriteOutcome outcome = outcomes.get(i);
            if (outcome == null) {
                routeRetryOrFailure(session, flowFile, retryPolicy);
            } else {
                switch (outcome) {
                    case SUCCESS -> session.transfer(flowFile, REL_SUCCESS);
                    case SKIPPED -> session.transfer(flowFile, REL_SKIPPED);
                    case CONFLICT -> session.transfer(flowFile, REL_FAILURE);
                }
            }
        }
        session.commit();
    }

    private static List<CommandBuilder.WriteOutcome> dispatchAndAwait(
            RedisClusterAsyncCommands<byte[], byte[]> cmds, StatefulConnection<byte[], byte[]> rawConn, List<PendingWrite> ready, String keyPrefix,
            String keyPrefixSeparator, CommandBuilder.TtlStrategy ttlStrategy, long ttlResetOffsetMs,
            CommandBuilder.ConflictStrategy conflictStrategy, int writeChunkSize, long batchTimeoutMs, ComponentLog logger) {
        List<CompletableFuture<CommandBuilder.WriteOutcome>> futures = new ArrayList<>(ready.size());
        for (PendingWrite pending : ready) {
            futures.add(CommandBuilder.write(cmds, rawConn, pending.record, keyPrefix, keyPrefixSeparator, ttlStrategy, ttlResetOffsetMs, conflictStrategy, writeChunkSize));
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(batchTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // At least one write in this batch failed or timed out - which one(s) is logged
            // below, per-future, since allOf's own exception doesn't identify which member failed.
            logger.warn("Batch of {} writes to Dragonfly did not all complete within {}ms: {}",
                    ready.size(), batchTimeoutMs, e.toString());
        }
        List<CommandBuilder.WriteOutcome> outcomes = new ArrayList<>(futures.size());
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<CommandBuilder.WriteOutcome> future = futures.get(i);
            if (future.isDone() && !future.isCompletedExceptionally()) {
                outcomes.add(future.join());
            } else {
                outcomes.add(null);
                String key = ready.get(i).record.key;
                if (future.isCompletedExceptionally()) {
                    Throwable cause = null;
                    try {
                        future.join();
                    } catch (Exception e) {
                        cause = e.getCause() != null ? e.getCause() : e;
                    }
                    logger.warn("Write failed for key {}", key, cause);
                } else {
                    logger.warn("Write for key {} did not complete within {}ms (still pending)", key, batchTimeoutMs);
                }
            }
        }
        return outcomes;
    }

    private void routeRetryOrFailure(ProcessSession session, FlowFile flowFile, RetryPolicy retryPolicy) {
        int attempt = 1 + parseInt(flowFile.getAttribute(RetryPolicy.RETRY_COUNT_ATTRIBUTE), 0);
        if (attempt > retryPolicy.maxAttempts()) {
            getLogger().warn("Giving up on {} after {} attempts, routing to failure", flowFile, attempt - 1);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        getLogger().warn("Routing {} to retry (attempt {} of {})", flowFile, attempt, retryPolicy.maxAttempts());
        Map<String, String> attrs = new HashMap<>();
        attrs.put(RetryPolicy.RETRY_COUNT_ATTRIBUTE, String.valueOf(attempt));
        attrs.put(RetryPolicy.RETRY_NOT_BEFORE_ATTRIBUTE, String.valueOf(System.currentTimeMillis() + retryPolicy.delayMillisForAttempt(attempt)));
        FlowFile updated = session.putAllAttributes(flowFile, attrs);
        session.transfer(updated, REL_RETRY);
    }

    private static void putIfSet(Map<String, Integer> map, String type, ProcessContext context, PropertyDescriptor descriptor) {
        if (context.getProperty(descriptor).isSet()) {
            map.put(type, context.getProperty(descriptor).asInteger());
        }
    }

    private static int parseInt(String value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
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

    private record PendingWrite(FlowFile flowFile, KeyRecord record) {
    }

    // ReadyBatchFilter buckets by Redis type: a type with its own perTypeLimits entry gets an
    // independent cap, fully separate from every other type; every type WITHOUT an entry shares
    // one combined "__default__" bucket capped at defaultBatchSize - exactly the single global
    // cap this filter enforced before per-type overrides existed, so an unconfigured migration
    // (perTypeLimits empty) behaves byte-for-byte the same as before. A type hitting its own cap
    // doesn't stop the scan (REJECT_AND_CONTINUE, not TERMINATE) when overrides are in play,
    // since a different type further back in the queue may still have room under its own cap or
    // the default bucket's - bounded by a generous scanned-count safety valve so a queue that's
    // mostly one already-capped type can't force scanning the entire backlog every trigger. With
    // no overrides configured there's only ever the one bucket, so hitting its cap can safely
    // terminate immediately, same as the original single-limit behavior.
    private static final class ReadyBatchFilter implements FlowFileFilter {
        private static final int MAX_SCAN_MULTIPLIER = 20;
        private static final String DEFAULT_BUCKET = "__default__";

        private final int defaultBatchSize;
        private final Map<String, Integer> perTypeLimits;
        private final long now;
        private final Map<String, Integer> acceptedByBucket = new HashMap<>();
        private int scanned = 0;

        ReadyBatchFilter(int defaultBatchSize, Map<String, Integer> perTypeLimits, long now) {
            this.defaultBatchSize = defaultBatchSize;
            this.perTypeLimits = perTypeLimits;
            this.now = now;
        }

        @Override
        public FlowFileFilterResult filter(FlowFile flowFile) {
            scanned++;
            String type = flowFile.getAttribute("redis.type");
            String typeKey = type == null ? "" : type.toLowerCase();
            Integer overrideLimit = perTypeLimits.get(typeKey);
            String bucket = overrideLimit != null ? typeKey : DEFAULT_BUCKET;
            int limit = overrideLimit != null ? overrideLimit : defaultBatchSize;
            int acceptedSoFar = acceptedByBucket.getOrDefault(bucket, 0);
            if (acceptedSoFar >= limit) {
                if (perTypeLimits.isEmpty() || scanned >= (long) defaultBatchSize * MAX_SCAN_MULTIPLIER) {
                    return FlowFileFilterResult.REJECT_AND_TERMINATE;
                }
                return FlowFileFilterResult.REJECT_AND_CONTINUE;
            }
            String notBefore = flowFile.getAttribute(RetryPolicy.RETRY_NOT_BEFORE_ATTRIBUTE);
            if (notBefore != null) {
                try {
                    if (Long.parseLong(notBefore) > now) {
                        return FlowFileFilterResult.REJECT_AND_CONTINUE;
                    }
                } catch (NumberFormatException ignored) {
                    // malformed attribute; treat as ready
                }
            }
            acceptedByBucket.put(bucket, acceptedSoFar + 1);
            return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
        }
    }
}
