package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.RedisPubSubHandle;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Tags({"redis", "migration", "live", "keyspace-notifications", "source"})
@CapabilityDescription("Subscribes to source Redis keyspace notifications, emitting one FlowFile per key "
        + "change event for the live-phase replication pipeline. Spec section 4.4.")
public class RedisKeyspaceEventConsumer extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .required(true)
            .identifiesControllerService(RedisConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor KEYSPACE_PATTERN = new PropertyDescriptor.Builder()
            .name("keyspace-pattern")
            .displayName("Keyspace Pattern")
            .required(true)
            .defaultValue("__keyevent@*__:*")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor EVENT_TYPES = new PropertyDescriptor.Builder()
            .name("event-types")
            .displayName("Event Types")
            .description("Comma-separated event types to include (e.g. set,del,expire,hset,lpush). Empty means all.")
            .required(false)
            .build();

    public static final PropertyDescriptor MAX_QUEUE_DEPTH = new PropertyDescriptor.Builder()
            .name("max-queue-depth")
            .displayName("Max Queue Depth")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();

    public static final PropertyDescriptor RECONNECT_BACKOFF_MS = new PropertyDescriptor.Builder()
            .name("reconnect-backoff-ms")
            .displayName("Reconnect Backoff (ms)")
            .description("Lettuce auto-reconnects the underlying connection on its own; this value only "
                    + "throttles how often disconnect warnings are logged/bulletined.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final Relationship REL_KEY_CHANGED = new Relationship.Builder().name("key_changed").description("Key was created or modified").build();
    public static final Relationship REL_KEY_DELETED = new Relationship.Builder().name("key_deleted").description("Key was deleted").build();
    public static final Relationship REL_KEY_EXPIRED = new Relationship.Builder().name("key_expired").description("Key expired on source").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Event parsing or queue overflow").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REDIS_CONNECTION_POOL, KEYSPACE_PATTERN, EVENT_TYPES, MAX_QUEUE_DEPTH, RECONNECT_BACKOFF_MS);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_KEY_CHANGED, REL_KEY_DELETED, REL_KEY_EXPIRED, REL_FAILURE);

    private static final Pattern KEYEVENT_CHANNEL = Pattern.compile("__keyevent@(\\d+)__:(.+)");
    private static final Pattern KEYSPACE_CHANNEL = Pattern.compile("__keyspace@(\\d+)__:(.+)");

    private record RawEvent(String channel, String message, long timestampMs) {
    }

    private BlockingQueue<RawEvent> eventQueue;
    private RedisPubSubHandle pubSubHandle;
    private Set<String> eventTypeFilter;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        RedisConnectionPoolService redisPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        Map<String, String> config = redisPool.withConnection(cmds -> {
            try {
                return cmds.configGet("notify-keyspace-events").get();
            } catch (Exception e) {
                throw new ProcessException("Unable to read notify-keyspace-events configuration", e);
            }
        });
        String value = config.getOrDefault("notify-keyspace-events", "");
        if (!value.contains("A") || !value.contains("E")) {
            throw new ProcessException("Source Redis has notify-keyspace-events='" + value + "'; keyspace "
                    + "notifications require at least 'AE' (or the specific classes covering your Event "
                    + "Types plus 'E'). Enable it with: CONFIG SET notify-keyspace-events AE (or set the "
                    + "equivalent parameter-group setting on managed services such as ElastiCache).");
        }

        int maxQueueDepth = context.getProperty(MAX_QUEUE_DEPTH).asInteger();
        eventQueue = new LinkedBlockingQueue<>(maxQueueDepth);
        eventTypeFilter = context.getProperty(EVENT_TYPES).isSet()
                ? Arrays.stream(context.getProperty(EVENT_TYPES).getValue().split(",")).map(String::trim).map(String::toLowerCase).filter(s -> !s.isEmpty()).collect(Collectors.toSet())
                : Set.of();

        pubSubHandle = redisPool.openPubSub();
        pubSubHandle.psubscribe(context.getProperty(KEYSPACE_PATTERN).getValue(), (channel, message) -> {
            boolean accepted = eventQueue.offer(new RawEvent(
                    new String(channel, StandardCharsets.UTF_8), new String(message, StandardCharsets.UTF_8), System.currentTimeMillis()));
            if (!accepted) {
                getLogger().warn("Keyspace event queue is full (max {}); dropping event", maxQueueDepth);
            }
        });
    }

    @OnStopped
    public void onStopped() {
        if (pubSubHandle != null) {
            pubSubHandle.close();
            pubSubHandle = null;
        }
        eventQueue = null;
        eventTypeFilter = null;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        if (eventQueue == null) {
            context.yield();
            return;
        }
        RawEvent event = eventQueue.poll();
        if (event == null) {
            context.yield();
            return;
        }

        ParsedEvent parsed = parse(event.channel(), event.message());
        if (parsed == null) {
            FlowFile flowFile = session.create();
            flowFile = session.putAttribute(flowFile, "redis.event.channel", event.channel());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        if (!eventTypeFilter.isEmpty() && !eventTypeFilter.contains(parsed.eventType.toLowerCase())) {
            return;
        }

        Map<String, String> attrs = new HashMap<>();
        attrs.put("redis.key", parsed.key);
        attrs.put("redis.event.type", parsed.eventType);
        attrs.put("redis.event.db", String.valueOf(parsed.database));
        attrs.put("redis.event.timestamp", String.valueOf(event.timestampMs()));

        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, attrs);
        session.transfer(flowFile, relationshipFor(parsed.eventType));
    }

    private Relationship relationshipFor(String eventType) {
        return switch (eventType.toLowerCase()) {
            case "del" -> REL_KEY_DELETED;
            case "expired" -> REL_KEY_EXPIRED;
            default -> REL_KEY_CHANGED;
        };
    }

    record ParsedEvent(String key, String eventType, int database) {
    }

    static ParsedEvent parse(String channel, String message) {
        Matcher keyevent = KEYEVENT_CHANNEL.matcher(channel);
        if (keyevent.matches()) {
            return new ParsedEvent(message, keyevent.group(2), Integer.parseInt(keyevent.group(1)));
        }
        Matcher keyspace = KEYSPACE_CHANNEL.matcher(channel);
        if (keyspace.matches()) {
            return new ParsedEvent(keyspace.group(2), message, Integer.parseInt(keyspace.group(1)));
        }
        return null;
    }
}
