package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.ReplicationEndpoint;
import io.dragonfly.nifi.redis.util.CommandKeyTable;
import io.dragonfly.nifi.redis.util.KeyFilter;
import io.dragonfly.nifi.redis.util.ReplicationStream;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Follows the source as a replica (PSYNC) and emits the keys that change, so nothing written to the
 * source is missed, including across a disconnect or a NiFi restart. Every changed key is emitted,
 * not the command, and RedisTypeDeserializer then copies the key's current value: replaying a
 * key's current value is safe to repeat, so a crash can only cause re-copies, never doubled
 * increments or pushes. Keys are coalesced while they wait, so the backlog grows with the number
 * of distinct changed keys rather than with write volume, and the replication socket is read
 * continuously whatever back-pressure the flow applies.
 *
 * <p>After a full resync the master's RDB is skipped and the keyspace is SCANned instead; the scan
 * starts after the handshake, so a write made before it is in the scan and a write made after it
 * is in the stream. The replication id and offset are saved to cluster state only once every key
 * changed up to that offset has been committed to the flow and the scan has finished.
 */
@Tags({"redis", "valkey", "migration", "replication", "psync", "live", "source"})
@CapabilityDescription("Connects to the source master as a replica (PSYNC), SCANs the keyspace after each full resync, "
        + "and emits one FlowFile per changed key (redis.key) for RedisTypeDeserializer. Resumes with a partial "
        + "resync from the saved replication id and offset after a disconnect or restart.")
@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = Scope.CLUSTER, description = "The source's replication id and the offset up to which every changed key "
        + "has been emitted, used to resume with PSYNC.")
public class RedisReplicationReader extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .description("The source. Standalone or Sentinel; the replica connection follows the Sentinel's current master.")
            .required(true)
            .identifiesControllerService(RedisConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor SCAN_COUNT = new PropertyDescriptor.Builder()
            .name("scan-count")
            .displayName("Snapshot Scan Count")
            .description("COUNT hint per SCAN call of the snapshot that follows a full resync.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor MAX_FLOWFILES_PER_TRIGGER = new PropertyDescriptor.Builder()
            .name("max-flowfiles-per-trigger")
            .displayName("Max FlowFiles per Trigger")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();

    public static final PropertyDescriptor MAX_PENDING_SNAPSHOT_KEYS = new PropertyDescriptor.Builder()
            .name("max-pending-snapshot-keys")
            .displayName("Max Pending Snapshot Keys")
            .description("The snapshot SCAN pauses while more keys than this wait to be emitted, so a large "
                    + "keyspace is never held in memory at once. Keys changed by the replication stream are "
                    + "always accepted; the stream is never paused.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("200000")
            .build();

    public static final PropertyDescriptor RECONNECT_BACKOFF_MS = new PropertyDescriptor.Builder()
            .name("reconnect-backoff-ms")
            .displayName("Reconnect Backoff (ms)")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor CONNECT_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("connect-timeout-ms")
            .displayName("Connect Timeout (ms)")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();

    public static final Relationship REL_KEY_CHANGED = new Relationship.Builder().name("key_changed")
            .description("A key that the snapshot found or a command changed; RedisTypeDeserializer copies its current value").build();
    public static final Relationship REL_FLUSH = new Relationship.Builder().name("flush")
            .description("FLUSHALL, FLUSHDB or SWAPDB on the replicated database. Keys written afterwards still arrive "
                    + "on key_changed, but the target only loses the flushed keys if something acts on this "
                    + "(redis.flush.command, redis.event.db)").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REDIS_CONNECTION_POOL, RedisScanReader.PREFIX_DENY_LIST, RedisScanReader.PREFIX_ONLY_LIST, SCAN_COUNT,
            MAX_FLOWFILES_PER_TRIGGER, MAX_PENDING_SNAPSHOT_KEYS, RECONNECT_BACKOFF_MS, CONNECT_TIMEOUT_MS);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_KEY_CHANGED, REL_FLUSH);

    static final String STATE_REPL_ID = "replid";
    static final String STATE_OFFSET = "offset";
    static final String STATE_DB = "db";

    private final AtomicLong fullResyncs = new AtomicLong();
    private final AtomicLong partialResyncs = new AtomicLong();
    private final AtomicLong disconnects = new AtomicLong();
    private final AtomicLong commands = new AtomicLong();
    private final AtomicLong prefixSkipped = new AtomicLong();
    private final AtomicLong otherDbCommands = new AtomicLong();
    private final AtomicLong unknownKeyCommands = new AtomicLong();

    /** Null while stopped. */
    private volatile Run run;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        RedisConnectionPoolService pool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        ReplicationEndpoint first = pool.replicationEndpoint().orElseThrow(() -> new ProcessException(
                "The connection pool cannot name a single master to replicate from; cluster sources are not supported yet"));
        KeyFilter filter = KeyFilter.parse(null, context.getProperty(RedisScanReader.PREFIX_DENY_LIST).getValue(),
                context.getProperty(RedisScanReader.PREFIX_ONLY_LIST).getValue());
        CommandKeyTable keyTable = CommandKeyTable.fromCommandReply(await(pool, cmds -> cmds.command()), argv -> getKeys(pool, argv));
        StateMap state = context.getStateManager().getState(Scope.CLUSTER);
        String savedReplId = state.get(STATE_REPL_ID);
        long savedOffset = savedReplId == null ? -1 : Long.parseLong(state.get(STATE_OFFSET));
        int savedDb = state.get(STATE_DB) == null ? -1 : Integer.parseInt(state.get(STATE_DB));

        Run started = new Run();
        int backoffMs = context.getProperty(RECONNECT_BACKOFF_MS).asInteger();
        int connectTimeoutMs = context.getProperty(CONNECT_TIMEOUT_MS).asInteger();
        int scanCount = context.getProperty(SCAN_COUNT).asInteger();
        int maxPending = context.getProperty(MAX_PENDING_SNAPSHOT_KEYS).asInteger();
        Thread thread = new Thread(() -> replicate(started, pool, first.database(), filter, keyTable, savedReplId, savedOffset, savedDb,
                backoffMs, connectTimeoutMs, scanCount, maxPending), "r2dfly-replication-" + getIdentifier());
        thread.setDaemon(true);
        started.replicationThread = thread;
        run = started;
        thread.start();
    }

    @OnStopped
    public void onStopped() throws InterruptedException {
        Run stopping = run;
        run = null;
        if (stopping != null) {
            stopping.stop();
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        reportCounters(session);
        Run current = run;
        if (current == null) {
            context.yield();
            return;
        }
        Dirty dirty = current.dirty;
        for (Map<String, String> flush : dirty.takeFlushes()) {
            session.transfer(session.putAllAttributes(session.create(), flush), REL_FLUSH);
        }
        Batch batch = dirty.take(context.getProperty(MAX_FLOWFILES_PER_TRIGGER).asInteger());
        if (batch.keys().isEmpty() && batch.checkpoint() == null) {
            session.commit();
            context.yield();
            return;
        }
        try {
            for (String key : batch.keys()) {
                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, Map.of("redis.key", key, "redis.event.db", String.valueOf(batch.db())));
                session.transfer(flowFile, REL_KEY_CHANGED);
            }
            session.commit();
        } catch (RuntimeException e) {
            dirty.giveBack(batch.keys());
            throw e;
        }
        if (batch.checkpoint() != null) {
            try {
                context.getStateManager().setState(Map.of(STATE_REPL_ID, batch.checkpoint().replId(),
                        STATE_OFFSET, String.valueOf(batch.checkpoint().offset()),
                        STATE_DB, String.valueOf(batch.checkpoint().db())), Scope.CLUSTER);
            } catch (IOException e) {
                getLogger().warn("Could not save the replication offset; a restart will resume from an earlier one", e);
            }
        }
    }

    private void replicate(Run run, RedisConnectionPoolService pool, int database, KeyFilter filter, CommandKeyTable keyTable,
                           String savedReplId, long savedOffset, int savedDb, int backoffMs, int connectTimeoutMs, int scanCount, int maxPending) {
        String replId = savedReplId;
        long offset = savedOffset;
        int streamDb = savedDb;
        Dirty dirty = run.dirty;
        while (run.running) {
            ReplicationStream opened = null;
            try {
                ReplicationEndpoint endpoint = pool.replicationEndpoint().orElseThrow();
                opened = ReplicationStream.open(endpoint, replId, offset, streamDb, connectTimeoutMs, run::connecting);
                if (opened.syncKind() == ReplicationStream.SyncKind.FULL) {
                    fullResyncs.incrementAndGet();
                    getLogger().info("Full resync from {}:{} (replication id {}, offset {}); scanning the keyspace",
                            endpoint.host(), endpoint.port(), opened.replId(), opened.offset());
                    dirty.fullResync(opened.replId(), opened.offset(), database);
                    startSnapshot(run, pool, filter, scanCount, maxPending, backoffMs);
                } else {
                    partialResyncs.incrementAndGet();
                    getLogger().info("Partial resync from {}:{} continues replication id {} after offset {}",
                            endpoint.host(), endpoint.port(), opened.replId(), offset);
                    dirty.continued(opened.replId(), database, offset, streamDb);
                }
                replId = opened.replId();
                opened.run((db, argv, offsetAfter) -> onCommand(dirty, database, db, argv, offsetAfter, filter, keyTable));
            } catch (IOException | RuntimeException e) {
                if (!run.running) {
                    return;
                }
                disconnects.incrementAndGet();
                getLogger().warn("Replication connection lost; reconnecting in {} ms", backoffMs, e);
            } finally {
                if (opened != null) {
                    offset = opened.offset();
                    streamDb = opened.db();
                }
                run.disconnect();
            }
            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void onCommand(Dirty dirty, int database, int db, List<byte[]> argv, long offsetAfter, KeyFilter filter, CommandKeyTable keyTable) {
        if (argv.isEmpty()) {
            dirty.advance(offsetAfter, db);
            return;
        }
        commands.incrementAndGet();
        String name = new String(argv.get(0), StandardCharsets.UTF_8).toLowerCase();
        if (name.equals("flushall") || (name.equals("swapdb") && touchesDb(argv, database))) {
            dirty.flush(name, database, offsetAfter, db);
            return;
        }
        if (db != database) {
            otherDbCommands.incrementAndGet();
            dirty.advance(offsetAfter, db);
            return;
        }
        if (name.equals("flushdb")) {
            dirty.flush(name, database, offsetAfter, db);
            return;
        }
        List<byte[]> keys;
        try {
            keys = keyTable.keys(argv);
        } catch (RuntimeException e) {
            unknownKeyCommands.incrementAndGet();
            getLogger().warn("Could not tell which keys {} touches; its change is not copied until the key changes again", name, e);
            dirty.advance(offsetAfter, db);
            return;
        }
        List<String> kept = new ArrayList<>(keys.size());
        for (byte[] key : keys) {
            if (filter.allowsKey(key)) {
                kept.add(new String(key, StandardCharsets.UTF_8));
            } else {
                prefixSkipped.incrementAndGet();
            }
        }
        dirty.mark(kept, offsetAfter, db);
    }

    private static boolean touchesDb(List<byte[]> argv, int database) {
        String db = String.valueOf(database);
        return argv.size() > 2 && (db.equals(new String(argv.get(1), StandardCharsets.US_ASCII))
                || db.equals(new String(argv.get(2), StandardCharsets.US_ASCII)));
    }

    private void startSnapshot(Run run, RedisConnectionPoolService pool, KeyFilter filter, int scanCount, int maxPending, int backoffMs) {
        Thread previous = run.snapshotThread;
        if (previous != null) {
            previous.interrupt();
        }
        Dirty dirty = run.dirty;
        long generation = dirty.generation();
        Thread thread = new Thread(() -> {
            try {
                ScanCursor cursor = ScanCursor.INITIAL;
                while (!Thread.currentThread().isInterrupted() && run.running) {
                    ScanCursor current = cursor;
                    KeyScanCursor<byte[]> page;
                    try {
                        page = await(pool, cmds -> cmds.scan(current, ScanArgs.Builder.limit(scanCount)));
                    } catch (RuntimeException e) {
                        // Retried from the same cursor: giving up would leave the rest of the keyspace
                        // uncopied with nothing to say so but the missing offset checkpoint.
                        getLogger().warn("Snapshot SCAN failed; retrying in {} ms", backoffMs, e);
                        Thread.sleep(backoffMs);
                        continue;
                    }
                    List<String> keys = new ArrayList<>();
                    for (byte[] key : page.getKeys()) {
                        if (filter.allowsKey(key)) {
                            keys.add(new String(key, StandardCharsets.UTF_8));
                        }
                    }
                    if (!dirty.markSnapshot(keys, generation)) {
                        return;
                    }
                    if (page.isFinished()) {
                        dirty.snapshotFinished(generation);
                        getLogger().info("Snapshot scan finished");
                        return;
                    }
                    cursor = page;
                    while (dirty.size() > maxPending && !Thread.currentThread().isInterrupted()) {
                        Thread.sleep(50);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "r2dfly-snapshot-" + getIdentifier());
        thread.setDaemon(true);
        run.snapshotThread = thread;
        thread.start();
    }

    private List<byte[]> getKeys(RedisConnectionPoolService pool, List<byte[]> argv) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).add("GETKEYS");
        argv.forEach(args::add);
        List<Object> reply = await(pool, cmds -> cmds.dispatch(CommandType.COMMAND, new ArrayOutput<>(ByteArrayCodec.INSTANCE), args));
        return reply.stream().map(k -> (byte[]) k).toList();
    }

    private static <T> T await(RedisConnectionPoolService pool,
                               java.util.function.Function<io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands<byte[], byte[]>, io.lettuce.core.RedisFuture<T>> call) {
        return pool.withConnection(cmds -> {
            try {
                return call.apply(cmds).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessException(e);
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        });
    }

    private void reportCounters(ProcessSession session) {
        drain(session, "Replication Full Resyncs", fullResyncs);
        drain(session, "Replication Partial Resyncs", partialResyncs);
        drain(session, "Replication Disconnects", disconnects);
        drain(session, "Replication Commands", commands);
        drain(session, "Replication Keys Skipped (Prefix Filter)", prefixSkipped);
        drain(session, "Replication Commands in Other Databases", otherDbCommands);
        drain(session, "Replication Commands With Unknown Keys", unknownKeyCommands);
    }

    private static void drain(ProcessSession session, String name, AtomicLong counter) {
        long value = counter.getAndSet(0);
        if (value != 0) {
            session.adjustCounter(name, value, false);
        }
    }

    /**
     * One schedule's state. A thread that outlives onStopped's join (stuck in a connect timeout,
     * say) only ever reaches its own Run, never the next schedule's dirty set or socket.
     */
    private static final class Run {
        final Dirty dirty = new Dirty();
        volatile boolean running = true;
        volatile Thread replicationThread;
        volatile Thread snapshotThread;
        private volatile Closeable connection;

        void connecting(Closeable socket) {
            connection = socket;
            if (!running) {
                closeQuietly(socket);
            }
        }

        void disconnect() {
            Closeable current = connection;
            connection = null;
            closeQuietly(current);
        }

        void stop() throws InterruptedException {
            running = false;
            disconnect();
            for (Thread thread : new Thread[]{replicationThread, snapshotThread}) {
                if (thread != null) {
                    thread.interrupt();
                    thread.join(10_000);
                }
            }
        }

        private static void closeQuietly(Closeable closeable) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException ignored) {
                    // closing is how a stop aborts it; a socket that is already broken is fine
                }
            }
        }
    }

    record Checkpoint(String replId, long offset, int db) {
    }

    record Batch(List<String> keys, int db, Checkpoint checkpoint) {
    }

    /**
     * The keys waiting to be emitted, guarded by one lock that the replication thread, the snapshot
     * thread and onTrigger share. A drain takes the whole pending set at once with the offset it
     * covers; that offset becomes saveable once the drained set is fully emitted.
     */
    private static final class Dirty {
        private LinkedHashSet<String> pending = new LinkedHashSet<>();
        private LinkedHashSet<String> draining = new LinkedHashSet<>();
        private final List<Map<String, String>> flushes = new ArrayList<>();
        private String replId;
        private long pendingOffset = -1;
        /** The stream's selected database at pendingOffset, saved with it for the resume. */
        private int streamDb = -1;
        private int db;
        private boolean snapshotDone;
        private long generation;
        private Checkpoint drainCheckpoint;
        private long lastSavedOffset = -1;

        /** A new snapshot supersedes whatever an earlier one had not finished. */
        synchronized void fullResync(String replId, long offset, int db) {
            this.replId = replId;
            this.pendingOffset = offset;
            this.db = db;
            this.snapshotDone = false;
            this.drainCheckpoint = null;
            this.generation++;
        }

        /** A partial resync keeps everything already pending; the stream picks up where it was. */
        synchronized void continued(String replId, int db, long resumedAfter, int resumedDb) {
            this.replId = replId;
            this.db = db;
            if (pendingOffset < 0) {
                pendingOffset = resumedAfter;
                streamDb = resumedDb;
                lastSavedOffset = resumedAfter;
                snapshotDone = true;
            }
        }

        synchronized long generation() {
            return generation;
        }

        synchronized void mark(List<String> keys, long offsetAfter, int atDb) {
            pending.addAll(keys);
            advance(offsetAfter, atDb);
        }

        synchronized void advance(long offsetAfter, int atDb) {
            pendingOffset = offsetAfter;
            streamDb = atDb;
        }

        synchronized void flush(String command, int db, long offsetAfter, int atDb) {
            flushes.add(Map.of("redis.flush.command", command, "redis.event.db", String.valueOf(db)));
            advance(offsetAfter, atDb);
        }

        /** False once a newer full resync has replaced the snapshot this generation belongs to. */
        synchronized boolean markSnapshot(List<String> keys, long forGeneration) {
            if (forGeneration != generation) {
                return false;
            }
            pending.addAll(keys);
            return true;
        }

        synchronized void snapshotFinished(long forGeneration) {
            if (forGeneration == generation) {
                snapshotDone = true;
            }
        }

        synchronized int size() {
            return pending.size() + draining.size();
        }

        synchronized List<Map<String, String>> takeFlushes() {
            List<Map<String, String>> taken = List.copyOf(flushes);
            flushes.clear();
            return taken;
        }

        synchronized Batch take(int max) {
            if (draining.isEmpty() && replId != null) {
                draining = pending;
                pending = new LinkedHashSet<>();
                // The master's PINGs keep moving an idle offset; saving it every trigger would only churn cluster state.
                drainCheckpoint = snapshotDone && pendingOffset != lastSavedOffset ? new Checkpoint(replId, pendingOffset, streamDb) : null;
            }
            List<String> keys = new ArrayList<>(Math.min(max, draining.size()));
            Iterator<String> it = draining.iterator();
            while (it.hasNext() && keys.size() < max) {
                keys.add(it.next());
                it.remove();
            }
            Checkpoint checkpoint = null;
            if (draining.isEmpty() && drainCheckpoint != null) {
                checkpoint = drainCheckpoint;
                drainCheckpoint = null;
                lastSavedOffset = checkpoint.offset();
            }
            return new Batch(keys, db, checkpoint);
        }

        synchronized void giveBack(List<String> keys) {
            LinkedHashSet<String> restored = new LinkedHashSet<>(keys);
            restored.addAll(draining);
            draining = restored;
        }
    }
}
