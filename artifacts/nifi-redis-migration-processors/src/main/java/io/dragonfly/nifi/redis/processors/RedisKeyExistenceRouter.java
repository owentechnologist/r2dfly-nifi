package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.FailureReason;
import io.dragonfly.nifi.redis.util.KeyFilter;
import io.dragonfly.nifi.redis.util.KeyMapping;
import io.dragonfly.nifi.redis.util.RedisBytes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Tags({"redis", "dragonfly", "migration", "reconciliation", "exists"})
@CapabilityDescription("Given a FlowFile carrying a target-side key in redis.key, checks whether that key "
        + "still exists on the source Redis and routes on the answer. Keys outside the migration's "
        + "configured prefix scope are routed to 'filtered' without contacting the source.")
public class RedisKeyExistenceRouter extends AbstractProcessor {

    static final String ORPHAN_COUNTER = "Orphan Keys Found";

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .description("The source Redis the incoming target-side keys are checked against.")
            .required(true)
            .identifiesControllerService(RedisConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor KEY_PREFIX = RedisBatchWriter.KEY_PREFIX;
    public static final PropertyDescriptor KEY_PREFIX_SEPARATOR = RedisBatchWriter.KEY_PREFIX_SEPARATOR;

    public static final PropertyDescriptor PREFIX_DENY_LIST = RedisScanReader.PREFIX_DENY_LIST;
    public static final PropertyDescriptor PREFIX_ONLY_LIST = RedisScanReader.PREFIX_ONLY_LIST;

    public static final Relationship REL_EXISTS = new Relationship.Builder().name("exists")
            .description("The key exists on both the target and the source").build();
    // Whether a missing-on-source key gets deleted, logged, or merely counted is the flow author's
    // call, so this relationship deliberately names the observation and not a remedy.
    public static final Relationship REL_MISSING = new Relationship.Builder().name("missing")
            .description("The key exists on the target but not on the source. redis.key is rewritten to the "
                    + "source key it was copied from, so a downstream processor can map it again.").build();
    public static final Relationship REL_FILTERED = new Relationship.Builder().name("filtered")
            .description("The key is outside the migration's configured prefix scope, so the source was "
                    + "never consulted").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("The EXISTS command failed for this key").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REDIS_CONNECTION_POOL, KeyMapping.KEY_RENAME, KEY_PREFIX, KEY_PREFIX_SEPARATOR, PREFIX_DENY_LIST, PREFIX_ONLY_LIST);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_EXISTS, REL_MISSING, REL_FILTERED, REL_FAILURE);

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
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        byte[] targetKey = RedisBytes.key(flowFile);
        if (targetKey == null || targetKey.length == 0) {
            getLogger().error("FlowFile is missing the redis.key attribute");
            session.transfer(session.penalize(FailureReason.tag(session, flowFile, "no redis.key attribute")), REL_FAILURE);
            return;
        }

        // Scope is resolved before the EXISTS call, not after. An out-of-scope key is not an absent
        // key, and asking the source about one would let a bare "0" reply present pre-existing
        // target data as a migration finding.
        Optional<byte[]> sourceKey = inScopeSourceKey(
                targetKey,
                KeyMapping.of(context, KEY_PREFIX, KEY_PREFIX_SEPARATOR),
                KeyFilter.parse(null, context.getProperty(PREFIX_DENY_LIST).getValue(), context.getProperty(PREFIX_ONLY_LIST).getValue()));
        if (sourceKey.isEmpty()) {
            session.transfer(flowFile, REL_FILTERED);
            return;
        }
        byte[] bareKey = sourceKey.get();

        RedisConnectionPoolService redisPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        try {
            long found = redisPool.withConnection(cmds -> {
                try {
                    return cmds.exists(bareKey).get();
                } catch (Exception e) {
                    throw new ProcessException("EXISTS failed for key " + RedisBytes.display(bareKey), e);
                }
            });
            if (found > 0) {
                session.transfer(flowFile, REL_EXISTS);
            } else {
                session.adjustCounter(ORPHAN_COUNTER, 1, true);
                FlowFile renamed = session.removeAttribute(flowFile, RedisBytes.KEY_BASE64);
                session.transfer(session.putAllAttributes(renamed, RedisBytes.keyAttributes(bareKey)), REL_MISSING);
            }
        } catch (Exception e) {
            getLogger().error("Failed to check existence of key {} on the source", RedisBytes.display(bareKey), e);
            session.transfer(session.penalize(FailureReason.tag(session, flowFile, "EXISTS on the source failed", e)), REL_FAILURE);
        }
    }

    /**
     * Maps a target-side key back to the source key it was written from, or empty when the key falls
     * outside the migration's scope. A key that does not carry the configured Key Prefix was not
     * written by this migration at all, so it is out of scope by the same rule as the deny/only lists.
     * Both lists are matched against the source key, so the same values configured on the forward
     * leg's RedisScanReader apply here unchanged.
     */
    static Optional<byte[]> inScopeSourceKey(byte[] targetKey, KeyMapping keyMapping, KeyFilter scope) {
        return keyMapping.toSource(targetKey).filter(scope::allowsKey);
    }
}
