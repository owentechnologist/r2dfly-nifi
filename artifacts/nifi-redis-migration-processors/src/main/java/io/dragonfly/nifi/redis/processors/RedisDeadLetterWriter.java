package io.dragonfly.nifi.redis.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"redis", "migration", "dead-letter", "failure"})
@CapabilityDescription("Terminal sink for failure relationships: writes FlowFile content and attributes "
        + "to a JSON file, emits an ERROR bulletin, and increments the migration.failures.total counter. "
        + "Spec section 8.2.")
public class RedisDeadLetterWriter extends AbstractProcessor {

    public static final PropertyDescriptor OUTPUT_DIRECTORY = new PropertyDescriptor.Builder()
            .name("output-directory")
            .displayName("Output Directory")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Dead-letter file written").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Could not write the dead-letter file").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(OUTPUT_DIRECTORY);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String FAILURE_COUNTER = "migration.failures.total";

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
        Path outputDir = Path.of(context.getProperty(OUTPUT_DIRECTORY).getValue());
        try {
            Files.createDirectories(outputDir);
            byte[] content;
            try (InputStream in = session.read(flowFile)) {
                content = in.readAllBytes();
            }
            Map<String, Object> record = new LinkedHashMap<>();
            record.put("attributes", flowFile.getAttributes());
            record.put("content_base64", Base64.getEncoder().encodeToString(content));

            Path file = outputDir.resolve(flowFile.getAttribute("uuid") + ".json");
            Files.write(file, OBJECT_MAPPER.writeValueAsBytes(record));

            getLogger().error("Migration failure dead-lettered: key={} reason={} file={}",
                    flowFile.getAttribute("redis.key"), flowFile.getAttribute("redis.incompatible.reason"), file);
            session.adjustCounter(FAILURE_COUNTER, 1, true);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to write dead-letter file for FlowFile {}", flowFile.getAttribute("uuid"), e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }
    }
}
