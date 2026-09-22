package io.dragonfly.nifi.redis.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dragonfly.nifi.redis.util.KeyRecord;
import io.dragonfly.nifi.redis.util.RedisTypeSerializer;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges any {@code RecordReaderFactory}-based NiFi source (a DB table via
 * {@code QueryDatabaseTableRecord}, Kafka via {@code ConsumeKafkaRecord}, Mongo/files via a
 * record reader) into the same {@code KeyRecord} JSON envelope {@code RedisTypeDeserializer}/
 * {@code RedisSingleKeyFetch} already produce, so {@code RedisBatchWriter} can write the result
 * to Dragonfly unchanged - as a hash or a native JSON document, per Target Type.
 */
@Tags({"redis", "dragonfly", "record", "ingest", "hash", "json"})
@CapabilityDescription("Converts each Record read from the configured Record Reader into a KeyRecord "
        + "(hash or json) and writes it as FlowFile content in the same envelope RedisBatchWriter reads, "
        + "so non-Redis sources (DB rows, Kafka messages, Mongo documents, files) can land in Dragonfly.")
public class RecordToKeyRecord extends AbstractProcessor {

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    public static final AllowableValue TARGET_TYPE_HASH = new AllowableValue("hash", "hash",
            "Flattens the record's own top-level fields into a Dragonfly hash. A field holding a nested "
                    + "Record/array/Map/List is stringified into that hash field's value - nested structure "
                    + "does not survive a flat hash; use json when the source has nested structure.");
    public static final AllowableValue TARGET_TYPE_JSON = new AllowableValue("json", "json",
            "Preserves the record's full nested structure as a native Dragonfly JSON document.");

    public static final PropertyDescriptor TARGET_TYPE = new PropertyDescriptor.Builder()
            .name("target-type")
            .displayName("Target Type")
            .required(true)
            .allowableValues(TARGET_TYPE_HASH, TARGET_TYPE_JSON)
            .defaultValue(TARGET_TYPE_HASH.getValue())
            .build();

    public static final PropertyDescriptor KEY_FORMAT = new PropertyDescriptor.Builder()
            .name("key-format")
            .displayName("Key Format")
            .description("The Dragonfly key to write each record under, e.g. orders:${id}. A ${token} "
                    + "is resolved first against this record's own dynamic RecordPath properties (below); "
                    + "anything left unresolved falls back to the FlowFile's own attributes via normal "
                    + "NiFi Expression Language.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor TTL_MS = new PropertyDescriptor.Builder()
            .name("ttl-ms")
            .displayName("TTL (ms)")
            .description("TTL to write on every key produced from this FlowFile, in milliseconds. "
                    + "Unset (the default) means no TTL.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success").description("One FlowFile per Record successfully converted").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure").description("The Record Reader failed, or a Record could not be converted; "
                    + "the original FlowFile is routed here unmodified").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(RECORD_READER, TARGET_TYPE, KEY_FORMAT, TTL_MS);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Keyed by RecordPath expression text (compiling is a pure function of that text) rather than
     * by property name, so a dynamic property whose value is edited still gets a fresh compile on
     * next use without needing an {@code @OnScheduled} cache reset. */
    private final Map<String, RecordPath> compiledPathCache = new ConcurrentHashMap<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyName) {
        return new PropertyDescriptor.Builder()
                .name(propertyName)
                .displayName(propertyName)
                .description("RecordPath expression evaluated per Record; its result becomes the ${" + propertyName + "} token for Key Format.")
                .required(false)
                .dynamic(true)
                .addValidator(new RecordPathValidator())
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile original = session.get();
        if (original == null) {
            return;
        }

        RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        String targetType = context.getProperty(TARGET_TYPE).getValue();
        PropertyValue ttlProperty = context.getProperty(TTL_MS);
        long ttlMs = ttlProperty.isSet() ? ttlProperty.evaluateAttributeExpressions(original).asLong() : 0L;

        Map<String, RecordPath> tokenPaths = new LinkedHashMap<>();
        for (Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic() && entry.getValue() != null) {
                tokenPaths.put(entry.getKey().getName(), compiledPathCache.computeIfAbsent(entry.getValue(), RecordPath::compile));
            }
        }

        List<FlowFile> created = new ArrayList<>();
        try (InputStream in = session.read(original);
             RecordReader reader = readerFactory.createRecordReader(original, in, getLogger())) {

            Record record;
            while ((record = reader.nextRecord()) != null) {
                Map<String, String> tokenValues = new HashMap<>();
                for (Map.Entry<String, RecordPath> pathEntry : tokenPaths.entrySet()) {
                    String value = firstValueAsString(pathEntry.getValue().evaluate(record));
                    if (value != null) {
                        tokenValues.put(pathEntry.getKey(), value);
                    }
                }

                String key = context.getProperty(KEY_FORMAT).evaluateAttributeExpressions(original, tokenValues).getValue();
                Object value = TARGET_TYPE_JSON.getValue().equals(targetType) ? convertToJsonValue(record) : convertToHash(record);
                KeyRecord keyRecord = new KeyRecord(key, targetType, ttlMs, null, value);

                FlowFile child = session.create(original);
                child = session.write(child, out -> RedisTypeSerializer.writeJson(keyRecord, out));
                created.add(child);
            }
        } catch (Exception e) {
            // A malformed Record (or a dynamic RecordPath/conversion failure partway through this
            // FlowFile's Records) fails the whole FlowFile rather than emitting a partial set of
            // successes - any children already created for it are discarded. This does not block
            // other FlowFiles: the exception is caught here, not rethrown, so future onTrigger calls
            // against the next queued FlowFile are unaffected.
            getLogger().error("Failed to convert Records from {} to KeyRecords", original, e);
            for (FlowFile child : created) {
                session.remove(child);
            }
            session.transfer(session.penalize(original), REL_FAILURE);
            return;
        }

        for (FlowFile child : created) {
            session.transfer(child, REL_SUCCESS);
        }
        session.remove(original);
    }

    private static String firstValueAsString(RecordPathResult result) {
        return result.getSelectedFields().findFirst().map(FieldValue::getValue).map(String::valueOf).orElse(null);
    }

    private static Map<String, Object> convertToHash(Record record) {
        Map<String, Object> hash = new LinkedHashMap<>();
        for (String fieldName : record.getSchema().getFieldNames()) {
            hash.put(fieldName, flattenForHash(record.getValue(fieldName)));
        }
        return hash;
    }

    /** A nested Record/array/Map/List can't be represented as one flat hash field, so it is
     * stringified as JSON instead - see {@link #TARGET_TYPE_HASH}'s description. This is a real
     * limitation: such a field does not round-trip as native structure on Dragonfly's side. */
    private static Object flattenForHash(Object raw) {
        Object converted = convertToJsonValue(raw);
        if (converted instanceof Map || converted instanceof List) {
            try {
                return OBJECT_MAPPER.writeValueAsString(converted);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return converted;
    }

    private static Map<String, Object> convertToJsonValue(Record record) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (String fieldName : record.getSchema().getFieldNames()) {
            map.put(fieldName, convertToJsonValue(record.getValue(fieldName)));
        }
        return map;
    }

    private static Object convertToJsonValue(Object raw) {
        if (raw instanceof Record childRecord) {
            return convertToJsonValue(childRecord);
        }
        if (raw instanceof Object[] array) {
            List<Object> list = new ArrayList<>(array.length);
            for (Object element : array) {
                list.add(convertToJsonValue(element));
            }
            return list;
        }
        if (raw instanceof Collection<?> collection) {
            List<Object> list = new ArrayList<>(collection.size());
            for (Object element : collection) {
                list.add(convertToJsonValue(element));
            }
            return list;
        }
        if (raw instanceof Map<?, ?> map) {
            Map<String, Object> converted = new LinkedHashMap<>();
            map.forEach((k, v) -> converted.put(String.valueOf(k), convertToJsonValue(v)));
            return converted;
        }
        return raw;
    }
}
