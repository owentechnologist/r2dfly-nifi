package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
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
                    + "bare, unprefixed key so a downstream processor can re-apply its own Key Prefix.").build();
    public static final Relationship REL_FILTERED = new Relationship.Builder().name("filtered")
            .description("The key is outside the migration's configured prefix scope, so the source was "
                    + "never consulted").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("The EXISTS command failed for this key").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REDIS_CONNECTION_POOL, KEY_PREFIX, KEY_PREFIX_SEPARATOR, PREFIX_DENY_LIST, PREFIX_ONLY_LIST);

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
        String targetKey = flowFile.getAttribute("redis.key");
        if (targetKey == null || targetKey.isEmpty()) {
            getLogger().error("FlowFile is missing the redis.key attribute");
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        // Scope is resolved before the EXISTS call, not after. An out-of-scope key is not an absent
        // key, and asking the source about one would let a bare "0" reply present pre-existing
        // target data as a migration finding.
        Optional<String> sourceKey = inScopeSourceKey(
                targetKey,
                context.getProperty(KEY_PREFIX).getValue(),
                context.getProperty(KEY_PREFIX_SEPARATOR).getValue(),
                parsePrefixList(context.getProperty(PREFIX_DENY_LIST).getValue()),
                parsePrefixList(context.getProperty(PREFIX_ONLY_LIST).getValue()));
        if (sourceKey.isEmpty()) {
            session.transfer(flowFile, REL_FILTERED);
            return;
        }
        String bareKey = sourceKey.get();

        RedisConnectionPoolService redisPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        try {
            long found = redisPool.withConnection(cmds -> {
                try {
                    return cmds.exists(bareKey.getBytes(StandardCharsets.UTF_8)).get();
                } catch (Exception e) {
                    throw new ProcessException("EXISTS failed for key " + bareKey, e);
                }
            });
            if (found > 0) {
                session.transfer(flowFile, REL_EXISTS);
            } else {
                session.adjustCounter(ORPHAN_COUNTER, 1, true);
                session.transfer(session.putAttribute(flowFile, "redis.key", bareKey), REL_MISSING);
            }
        } catch (Exception e) {
            getLogger().error("Failed to check existence of key {} on the source", bareKey, e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }
    }

    /**
     * Maps a target-side key back to the source key it was written from, or empty when the key falls
     * outside the migration's scope. A key that does not carry the configured Key Prefix was not
     * written by this migration at all, so it is out of scope by the same rule as the deny/only lists.
     * Both lists are matched against the unprefixed key, so the same values configured on the forward
     * leg's RedisScanReader apply here unchanged.
     */
    static Optional<String> inScopeSourceKey(String targetKey, String prefix, String separator,
                                             List<String> denyPrefixes, List<String> onlyPrefixes) {
        String bareKey = targetKey;
        if (prefix != null && !prefix.isEmpty()) {
            String qualified = prefix + separator;
            if (!targetKey.startsWith(qualified)) {
                return Optional.empty();
            }
            bareKey = targetKey.substring(qualified.length());
        }
        if (startsWithAny(bareKey, denyPrefixes)) {
            return Optional.empty();
        }
        if (!onlyPrefixes.isEmpty() && !startsWithAny(bareKey, onlyPrefixes)) {
            return Optional.empty();
        }
        return Optional.of(bareKey);
    }

    static List<String> parsePrefixList(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    private static boolean startsWithAny(String key, List<String> prefixes) {
        return prefixes.stream().anyMatch(key::startsWith);
    }
}
