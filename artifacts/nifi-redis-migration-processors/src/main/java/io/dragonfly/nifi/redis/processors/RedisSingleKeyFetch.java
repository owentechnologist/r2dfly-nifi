package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.CommandBuilder;
import io.dragonfly.nifi.redis.util.FailureReason;
import io.dragonfly.nifi.redis.util.KeyFilter;
import io.dragonfly.nifi.redis.util.KeyRecord;
import io.dragonfly.nifi.redis.util.RedisBytes;
import io.dragonfly.nifi.redis.util.RedisTypeSerializer;
import io.dragonfly.nifi.redis.util.RedisValueFetcher;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Tags({"redis", "migration", "live", "source"})
@CapabilityDescription("Given a FlowFile with a redis.key attribute (from RedisKeyspaceEventConsumer), "
        + "fetches the key's current value and serialises it identically to RedisTypeDeserializer. Spec "
        + "section 4.5 - a thin TYPE-then-fetch wrapper reusing RedisValueFetcher's type dispatch.")
public class RedisSingleKeyFetch extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .required(true)
            .identifiesControllerService(RedisConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor KEY_TYPE_FILTER = new PropertyDescriptor.Builder()
            .name("key-type-filter")
            .displayName("Key Type Filter")
            .description("Comma-separated list of types to fetch, matched case-insensitively against TYPE "
                    + "(for example string,hash,ReJSON-RL,TopK-TYPE). A key of any other type is dropped and "
                    + "counted. Set it to the same value as RedisScanReader's Key Type Filter so the live "
                    + "phase copies the same keys as the scan. Leave unset to fetch every type.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = RedisTypeDeserializer.REL_SUCCESS;
    public static final Relationship REL_KEY_MISSING = RedisTypeDeserializer.REL_KEY_MISSING;
    public static final Relationship REL_MODULE_TYPE = RedisTypeDeserializer.REL_MODULE_TYPE;
    public static final Relationship REL_FAILURE = RedisTypeDeserializer.REL_FAILURE;

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REDIS_CONNECTION_POOL, KEY_TYPE_FILTER, RedisTypeDeserializer.HASH_FIELD_BATCH_SIZE, RedisTypeDeserializer.LIST_CHUNK_SIZE,
            RedisTypeDeserializer.STREAM_READ_COUNT, RedisTypeDeserializer.INCLUDE_CONSUMER_GROUPS);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_KEY_MISSING, REL_MODULE_TYPE, REL_FAILURE);

    private static final Set<String> CORE_TYPES = Set.of("string", "hash", "list", "set", "zset", "stream");

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private volatile RedisValueFetcher.FieldTtlSupport fieldTtls = new RedisValueFetcher.FieldTtlSupport();

    @OnScheduled
    public void onScheduled() {
        fieldTtls = new RedisValueFetcher.FieldTtlSupport();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        byte[] key = RedisBytes.key(flowFile);
        if (key == null || key.length == 0) {
            getLogger().error("FlowFile is missing the redis.key attribute");
            session.transfer(session.penalize(FailureReason.tag(session, flowFile, "no redis.key attribute")), REL_FAILURE);
            return;
        }

        RedisConnectionPoolService redisPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        int hashFieldBatchSize = context.getProperty(RedisTypeDeserializer.HASH_FIELD_BATCH_SIZE).asInteger();
        int listChunkSize = context.getProperty(RedisTypeDeserializer.LIST_CHUNK_SIZE).asInteger();
        int streamReadCount = context.getProperty(RedisTypeDeserializer.STREAM_READ_COUNT).asInteger();
        boolean includeConsumerGroups = context.getProperty(RedisTypeDeserializer.INCLUDE_CONSUMER_GROUPS).asBoolean();
        KeyFilter filter = KeyFilter.parse(context.getProperty(KEY_TYPE_FILTER).getValue(), null, null);

        try {
            TypeAndTtl meta = redisPool.withConnection(cmds -> {
                try {
                    var type = cmds.type(key);
                    var ttl = cmds.pttl(key);
                    return new TypeAndTtl(type.get(), ttl.get());
                } catch (Exception e) {
                    throw new ProcessException("TYPE/PTTL failed for key " + RedisBytes.display(key), e);
                }
            });
            String type = meta.type();

            if ("none".equals(type)) {
                session.transfer(flowFile, REL_KEY_MISSING);
                return;
            }
            if (!filter.allowsType(type)) {
                session.adjustCounter("Keys Skipped by Type Filter (" + type + ")", 1, false);
                session.remove(flowFile);
                return;
            }
            flowFile = session.putAllAttributes(flowFile, Map.of("redis.ttl.ms", String.valueOf(meta.ttlMs()),
                    CommandBuilder.TTL_READ_AT, String.valueOf(System.currentTimeMillis())));
            if (!CORE_TYPES.contains(type)) {
                flowFile = session.putAttribute(flowFile, "redis.type", type);
                session.transfer(flowFile, REL_MODULE_TYPE);
                return;
            }

            Optional<List<KeyRecord>> records = redisPool.withConnection(cmds -> RedisValueFetcher.fetch(
                    cmds, key, type, meta.ttlMs(), null, hashFieldBatchSize, listChunkSize, streamReadCount, includeConsumerGroups, fieldTtls));

            if (records.isEmpty()) {
                session.transfer(flowFile, REL_KEY_MISSING);
                return;
            }

            flowFile = session.putAttribute(flowFile, "redis.type", type);
            List<KeyRecord> chunks = records.get();
            for (int i = 0; i < chunks.size(); i++) {
                FlowFile child = session.create(flowFile);
                KeyRecord record = chunks.get(i);
                try {
                    child = session.write(child, out -> RedisTypeSerializer.writeJson(record, out));
                    if (chunks.size() > 1) {
                        child = session.putAttribute(child, "redis.chunk.index", String.valueOf(i));
                        child = session.putAttribute(child, "redis.chunk.total", String.valueOf(chunks.size()));
                    }
                    session.transfer(child, REL_SUCCESS);
                } catch (Exception e) {
                    session.remove(child);
                    throw e;
                }
            }
            session.remove(flowFile);
        } catch (Exception e) {
            getLogger().error("Failed to fetch key {}", key, e);
            session.transfer(session.penalize(FailureReason.tag(session, flowFile, "reading the key from the source failed", e)), REL_FAILURE);
        }
    }

    private record TypeAndTtl(String type, long ttlMs) {
    }
}
