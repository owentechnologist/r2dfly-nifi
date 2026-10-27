package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.util.ChunkReassembler;
import io.dragonfly.nifi.redis.util.FailureReason;
import io.dragonfly.nifi.redis.util.KeyRecord;
import io.dragonfly.nifi.redis.util.RedisTypeSerializer;
import io.dragonfly.nifi.redis.util.RespRecorder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Tags({"redis", "dragonfly", "file", "backup", "jsonl", "resp", "aof"})
@CapabilityDescription("Appends each key record to a file instead of writing it to a target: as one JSON line "
        + "per key (read back by KeyRecordFileReader), or as the RESP commands RedisBatchWriter would send, "
        + "which is also a single-file AOF. Chunks of a large key are joined first, so each key is one line "
        + "or one run of commands. TTLs count from when the file is loaded.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
// One writer per file: lines from concurrent tasks could interleave.
@TriggerSerially
public class KeyRecordFileWriter extends AbstractProcessor {

    public static final AllowableValue FORMAT_JSONL = new AllowableValue("jsonl", "JSON lines",
            "One key record per line, in the envelope the flow carries. KeyRecordFileReader reads it back.");
    public static final AllowableValue FORMAT_RESP = new AllowableValue("resp", "RESP commands",
            "The commands that rebuild each key. Loads with redis-cli --pipe into Redis, or as an AOF.");

    public static final PropertyDescriptor FILE = new PropertyDescriptor.Builder()
            .name("file")
            .displayName("File")
            .description("The file to append to. Its directory is created if missing.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FORMAT = new PropertyDescriptor.Builder()
            .name("format")
            .displayName("Format")
            .required(true)
            .allowableValues(FORMAT_JSONL, FORMAT_RESP)
            .defaultValue(FORMAT_JSONL.getValue())
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("Batch Size")
            .description("Key records appended and synced to disk per session.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("A key record that cannot be written to a file, with redis.failure.reason").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(FILE, FORMAT, BATCH_SIZE);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_FAILURE);

    private final ChunkReassembler chunks = new ChunkReassembler();
    private volatile FileChannel channel;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void open(ProcessContext context) throws IOException {
        Path file = Path.of(context.getProperty(FILE).getValue()).toAbsolutePath();
        Files.createDirectories(file.getParent());
        channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    @OnStopped
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        List<FlowFile> batch = session.get(context.getProperty(BATCH_SIZE).asInteger());
        if (batch.isEmpty()) {
            context.yield();
            return;
        }
        boolean resp = FORMAT_RESP.getValue().equals(context.getProperty(FORMAT).getValue());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long written = 0;
        for (FlowFile flowFile : batch) {
            if ("true".equals(flowFile.getAttribute("redis.dfly-dump"))) {
                session.transfer(FailureReason.tag(session, flowFile,
                        "a DUMP payload cannot be written to a file; copy without the RESTORE fast path"), REL_FAILURE);
                continue;
            }
            Optional<KeyRecord> record;
            try (InputStream in = session.read(flowFile)) {
                record = complete(RedisTypeSerializer.readJson(in), flowFile);
                if (record.isPresent()) {
                    out.write(resp ? RespRecorder.encode(record.get()) : jsonLine(record.get()));
                    written++;
                }
            } catch (Exception e) {
                session.transfer(FailureReason.tag(session, flowFile, "rendering the key record failed", e), REL_FAILURE);
                continue;
            }
            session.remove(flowFile);
        }
        try {
            ByteBuffer bytes = ByteBuffer.wrap(out.toByteArray());
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(false);
        } catch (IOException e) {
            throw new ProcessException("Appending to " + context.getProperty(FILE).getValue() + " failed", e);
        }
        session.adjustCounter("File Records Written", written, false);
    }

    private Optional<KeyRecord> complete(KeyRecord record, FlowFile flowFile) {
        String index = flowFile.getAttribute("redis.chunk.index");
        String total = flowFile.getAttribute("redis.chunk.total");
        if (index == null || total == null) {
            return Optional.of(record);
        }
        return chunks.accumulate(record.key, record, Integer.parseInt(index), Integer.parseInt(total));
    }

    private static byte[] jsonLine(KeyRecord record) {
        try {
            byte[] json = RedisTypeSerializer.toJsonBytes(record);
            byte[] line = new byte[json.length + 1];
            System.arraycopy(json, 0, line, 0, json.length);
            line[json.length] = '\n';
            return line;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
