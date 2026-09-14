package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.DragonflyConnectionPoolService;
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
import java.util.List;
import java.util.Set;

@Tags({"redis", "dragonfly", "migration", "live", "target"})
@CapabilityDescription("Deletes a key on the Dragonfly target for key_deleted/key_expired events from "
        + "RedisKeyspaceEventConsumer, per spec section 4.4's relationship table.")
public class DeleteRedisKey extends AbstractProcessor {

    public static final PropertyDescriptor DRAGONFLY_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("dragonfly-connection-pool")
            .displayName("Dragonfly Connection Pool")
            .required(true)
            .identifiesControllerService(DragonflyConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor KEY_PREFIX = RedisBatchWriter.KEY_PREFIX;
    public static final PropertyDescriptor KEY_PREFIX_SEPARATOR = RedisBatchWriter.KEY_PREFIX_SEPARATOR;

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Key deleted (or already absent) on the target").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Delete failed").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(DRAGONFLY_CONNECTION_POOL, KEY_PREFIX, KEY_PREFIX_SEPARATOR);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

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
        String key = flowFile.getAttribute("redis.key");
        if (key == null || key.isEmpty()) {
            getLogger().error("FlowFile is missing the redis.key attribute");
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        String prefix = context.getProperty(KEY_PREFIX).getValue();
        String separator = context.getProperty(KEY_PREFIX_SEPARATOR).getValue();
        byte[] targetKey = ((prefix == null || prefix.isEmpty()) ? key : prefix + separator + key).getBytes(StandardCharsets.UTF_8);

        DragonflyConnectionPoolService dragonflyPool = context.getProperty(DRAGONFLY_CONNECTION_POOL).asControllerService(DragonflyConnectionPoolService.class);
        try {
            dragonflyPool.withConnection(cmds -> {
                try {
                    return cmds.del(targetKey).get();
                } catch (Exception e) {
                    throw new ProcessException("DEL failed for key " + key, e);
                }
            });
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to delete key {}", key, e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }
    }
}
