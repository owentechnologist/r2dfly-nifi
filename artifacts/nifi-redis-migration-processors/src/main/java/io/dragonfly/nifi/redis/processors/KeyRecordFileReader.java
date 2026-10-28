package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.util.RedisBytes;
import io.dragonfly.nifi.redis.util.CounterGauge;
import io.dragonfly.nifi.redis.util.FailureReason;
import io.dragonfly.nifi.redis.util.KeyRecord;
import io.dragonfly.nifi.redis.util.RedisTypeSerializer;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"redis", "dragonfly", "file", "restore", "jsonl"})
@CapabilityDescription("Reads a JSON-lines file written by KeyRecordFileWriter and emits one key record per "
        + "line, for RedisBatchWriter. The read position is kept in local state, so a restart continues "
        + "where it stopped; changing File starts over. File Read Complete is 1 once the end is reached.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@Stateful(scopes = Scope.LOCAL, description = "The file being read and the byte offset of the next line.")
public class KeyRecordFileReader extends AbstractProcessor {

    public static final PropertyDescriptor FILE = new PropertyDescriptor.Builder()
            .name("file")
            .displayName("File")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORDS_PER_TRIGGER = new PropertyDescriptor.Builder()
            .name("records-per-trigger")
            .displayName("Records Per Trigger")
            .description("Lines read and emitted per session.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("One key record per line").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("A line that is not a key record, with redis.failure.reason").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(FILE, RECORDS_PER_TRIGGER);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);
    private static final String STATE_FILE = "file";
    private static final String STATE_OFFSET = "offset";
    private static final String COMPLETE_GAUGE = "File Read Complete";

    private final CounterGauge gauges = new CounterGauge();

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
        String file = context.getProperty(FILE).getValue();
        int limit = context.getProperty(RECORDS_PER_TRIGGER).asInteger();
        long offset;
        try {
            Map<String, String> state = session.getState(Scope.LOCAL).toMap();
            offset = file.equals(state.get(STATE_FILE)) ? Long.parseLong(state.get(STATE_OFFSET)) : 0;
        } catch (IOException e) {
            throw new ProcessException("Reading the saved file position failed", e);
        }

        long read = 0;
        boolean atEnd;
        try (FileChannel channel = FileChannel.open(Path.of(file), StandardOpenOption.READ)) {
            channel.position(offset);
            InputStream in = new BufferedInputStream(Channels.newInputStream(channel), 1 << 16);
            ByteArrayOutputStream line = new ByteArrayOutputStream();
            while (read < limit) {
                long lineStart = offset;
                line.reset();
                int b;
                while ((b = in.read()) != -1 && b != '\n') {
                    line.write(b);
                }
                if (b == -1 && line.size() == 0) {
                    break;
                }
                offset += line.size() + (b == '\n' ? 1 : 0);
                if (line.size() > 0) {
                    emit(session, line.toByteArray(), file, lineStart);
                    read++;
                }
            }
            atEnd = offset >= channel.size();
        } catch (IOException e) {
            throw new ProcessException("Reading " + file + " failed", e);
        }

        try {
            session.setState(Map.of(STATE_FILE, file, STATE_OFFSET, Long.toString(offset)), Scope.LOCAL);
        } catch (IOException e) {
            throw new ProcessException("Saving the file position failed", e);
        }
        session.adjustCounter("File Records Read", read, false);
        gauges.set(session, COMPLETE_GAUGE, atEnd ? 1 : 0);
        if (read == 0) {
            context.yield();
        }
    }

    private static void emit(ProcessSession session, byte[] line, String file, long lineStart) {
        FlowFile flowFile = session.write(session.create(), out -> out.write(line));
        flowFile = session.putAttribute(flowFile, "redis.source.file", file + "@" + lineStart);
        try {
            KeyRecord record = RedisTypeSerializer.readJson(line);
            if (record.key == null || record.type == null) {
                throw new IOException("the line has no key or no type");
            }
            flowFile = session.putAllAttributes(flowFile, RedisBytes.keyAttributes(record.keyBytes()));
            flowFile = session.putAttribute(flowFile, "redis.type", record.type);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (IOException e) {
            session.transfer(FailureReason.tag(session, flowFile, "the line at byte " + lineStart + " is not a key record", e), REL_FAILURE);
        }
    }
}
