package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.util.ChunkReassembler;
import io.dragonfly.nifi.redis.util.FailureReason;
import io.dragonfly.nifi.redis.util.KeyRecord;
import io.dragonfly.nifi.redis.util.RedisBytes;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        List<FlowFile> batch = session.get(ChunkReassembler.batchOf(context.getProperty(BATCH_SIZE).asInteger()));
        if (batch.isEmpty()) {
            context.yield();
            return;
        }
        boolean resp = FORMAT_RESP.getValue().equals(context.getProperty(FORMAT).getValue());
        List<ChunkReassembler.Chunk> parsed = new ArrayList<>();
        for (FlowFile flowFile : batch) {
            if ("true".equals(flowFile.getAttribute("redis.dfly-dump"))) {
                session.transfer(FailureReason.tag(session, flowFile,
                        "a DUMP payload cannot be written to a file; copy without the RESTORE fast path"), REL_FAILURE);
                continue;
            }
            try (InputStream in = session.read(flowFile)) {
                parsed.add(new ChunkReassembler.Chunk(flowFile, RedisTypeSerializer.readJson(in)));
            } catch (Exception e) {
                session.transfer(FailureReason.tag(session, flowFile, "parsing the key record failed", e), REL_FAILURE);
            }
        }
        Map<String, List<ChunkReassembler.Chunk>> groups = ChunkReassembler.byKey(
                parsed.stream().filter(c -> ChunkReassembler.isChunk(c.flowFile())).toList());
        Map<String, Integer> lastChunkAt = new HashMap<>();
        for (int i = 0; i < parsed.size(); i++) {
            if (ChunkReassembler.isChunk(parsed.get(i).flowFile())) {
                lastChunkAt.put(RedisBytes.identity(parsed.get(i).record().keyBytes()), i);
            }
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long written = 0;
        for (int i = 0; i < parsed.size(); i++) {
            ChunkReassembler.Chunk item = parsed.get(i);
            if (ChunkReassembler.isChunk(item.flowFile())) {
                String key = RedisBytes.identity(item.record().keyBytes());
                if (lastChunkAt.get(key) != i) {
                    continue;
                }
                Optional<ChunkReassembler.Chunk> joined = ChunkReassembler.join(session, groups.get(key), REL_FAILURE);
                if (joined.isEmpty()) {
                    continue;
                }
                item = joined.get();
            }
            try {
                out.write(resp ? RespRecorder.encode(item.record()) : jsonLine(item.record()));
                written++;
                session.remove(item.flowFile());
            } catch (Exception e) {
                session.transfer(FailureReason.tag(session, item.flowFile(), "rendering the key record failed", e), REL_FAILURE);
            }
        }
        append(context, out.toByteArray());
        session.adjustCounter("File Records Written", written, false);
    }

    /** All of a batch or none of it: a failed append is cut back, so the retried batch does not follow torn bytes. */
    private void append(ProcessContext context, byte[] bytes) {
        long sizeBefore = -1;
        try {
            sizeBefore = channel.size();
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(false);
        } catch (IOException e) {
            if (sizeBefore >= 0) {
                try {
                    channel.truncate(sizeBefore);
                } catch (IOException truncateFailed) {
                    e.addSuppressed(truncateFailed);
                }
            }
            throw new ProcessException("Appending to " + context.getProperty(FILE).getValue() + " failed", e);
        }
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
