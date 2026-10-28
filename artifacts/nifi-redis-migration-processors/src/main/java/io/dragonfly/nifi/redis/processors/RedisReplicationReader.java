package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.services.ReplicationEndpoint;
import io.dragonfly.nifi.redis.util.RedisBytes;
import io.dragonfly.nifi.redis.util.CommandKeyTable;
import io.dragonfly.nifi.redis.util.CounterGauge;
import io.dragonfly.nifi.redis.util.KeyFilter;
import io.dragonfly.nifi.redis.util.ReplicationStream;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Follows the source as a replica (PSYNC) and emits the keys that change, so nothing written to the
 * source is missed, including across a disconnect, a NiFi restart or a failover. Every changed key is emitted,
 * not the command, and RedisTypeDeserializer then copies the key's current value: replaying a
 * key's current value is safe to repeat, so a crash can only cause re-copies, never doubled
 * increments or pushes. Keys are coalesced while they wait, so the backlog grows with the number
 * of distinct changed keys rather than with write volume, and the replication socket is read
 * continuously whatever back-pressure the flow applies.
 *
 * <p>After a full resync the master's RDB is skipped and its keys are SCANned instead; the scan
 * starts after the handshake, so a write made before it is in the scan and a write made after it
 * is in the stream. The replication id and offset are saved to cluster state only once every key
 * changed up to that offset has been committed to the flow and the scan has finished.
 *
 * <p>A cluster source has one stream per slot-owning master, checked against the topology every
 * few seconds. A replica promoted by a failover inherits its old master's replication history
 * (PSYNC2), so its stream continues from the old one's offset; a new master gets a full resync of
 * its own keys only. Keys moved by resharding need nothing special: MIGRATE reaches the source
 * shard's stream as DEL and the destination's as RESTORE-ASKING, and both make the key re-read.
 */
@Tags({"redis", "valkey", "migration", "replication", "psync", "live", "source"})
@CapabilityDescription("Connects to the source master as a replica (PSYNC), one stream per master of a cluster, SCANs "
        + "a master's keys after each full resync, and emits one FlowFile per changed key (redis.key) for "
        + "RedisTypeDeserializer. Resumes with a partial resync from the saved replication id and offset after a "
        + "disconnect, a restart or a failover; follows masters that join or leave a cluster.")
@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = Scope.CLUSTER, description = "Per shard (stream.<master node id>, or stream.- outside a cluster): the "
        + "replication id, and the offset up to which every changed key has been emitted, used to resume with PSYNC.")
public class RedisReplicationReader extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .description("The source. Standalone or Sentinel, whose current master the replica connection follows, or "
                    + "a cluster, whose every slot-owning master gets a replica connection.")
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
            .description("FLUSHALL, FLUSHDB or SWAPDB on the replicated database (redis.flush.command, redis.event.db). "
                    + "Keys written afterwards still arrive on key_changed; the flushed keys leave the target when "
                    + "MigrationReconciler sees the DBSIZE drop and runs a full pass.").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REDIS_CONNECTION_POOL, RedisScanReader.PREFIX_DENY_LIST, RedisScanReader.PREFIX_ONLY_LIST, SCAN_COUNT,
            MAX_FLOWFILES_PER_TRIGGER, MAX_PENDING_SNAPSHOT_KEYS, RECONNECT_BACKOFF_MS, CONNECT_TIMEOUT_MS);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_KEY_CHANGED, REL_FLUSH);

    /** Before clusters were supported, the one stream's position was saved under these three keys. */
    static final String LEGACY_REPL_ID = "replid";
    static final String LEGACY_OFFSET = "offset";
    static final String LEGACY_DB = "db";
    static final String STATE_STREAM_PREFIX = "stream.";
    private static final long TOPOLOGY_CHECK_MS = 5_000;

    private final AtomicLong fullResyncs = new AtomicLong();
    private final AtomicLong partialResyncs = new AtomicLong();
    private final AtomicLong disconnects = new AtomicLong();
    private final AtomicLong commands = new AtomicLong();
    private final AtomicLong prefixSkipped = new AtomicLong();
    private final AtomicLong otherDbCommands = new AtomicLong();
    private final AtomicLong unknownKeyCommands = new AtomicLong();
    private final AtomicLong failoverResumes = new AtomicLong();
    private final AtomicLong newShards = new AtomicLong();
    private final AtomicLong retiredShards = new AtomicLong();

    private static final Pattern MASTER_REPL_OFFSET = Pattern.compile("(?m)^master_repl_offset:(\\d+)");

    /** Null while stopped. */
    private volatile Run run;
    private final CounterGauge gauges = new CounterGauge();

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
        Map<String, ReplicationEndpoint> first = pool.replicationEndpoints();
        if (first.isEmpty()) {
            throw new ProcessException("The connection pool names no master to replicate from");
        }
        KeyFilter filter = KeyFilter.parse(null, context.getProperty(RedisScanReader.PREFIX_DENY_LIST).getValue(),
                context.getProperty(RedisScanReader.PREFIX_ONLY_LIST).getValue());
        CommandKeyTable keyTable = CommandKeyTable.fromCommandReply(await(pool, cmds -> cmds.command()), argv -> getKeys(pool, argv));
        Run started = new Run(new Setup(pool, first.values().iterator().next().database(), filter, keyTable,
                context.getProperty(RECONNECT_BACKOFF_MS).asInteger(), context.getProperty(CONNECT_TIMEOUT_MS).asInteger(),
                context.getProperty(SCAN_COUNT).asInteger(), context.getProperty(MAX_PENDING_SNAPSHOT_KEYS).asInteger()));
        started.dirty.restore(savedPositions(context.getStateManager().getState(Scope.CLUSTER)));
        started.desired = first;
        Thread thread = new Thread(() -> supervise(started), "r2dfly-replication-" + getIdentifier());
        thread.setDaemon(true);
        started.supervisor = thread;
        run = started;
        thread.start();
    }

    static Map<String, Position> savedPositions(StateMap state) {
        Map<String, Position> saved = new java.util.TreeMap<>();
        if (state.get(LEGACY_REPL_ID) != null) {
            saved.put(RedisConnectionPoolService.STANDALONE_SHARD, new Position(state.get(LEGACY_REPL_ID),
                    Long.parseLong(state.get(LEGACY_OFFSET)), state.get(LEGACY_DB) == null ? -1 : Integer.parseInt(state.get(LEGACY_DB))));
        }
        state.toMap().forEach((name, value) -> {
            if (name.startsWith(STATE_STREAM_PREFIX)) {
                String[] parts = value.split("\\|");
                saved.put(name.substring(STATE_STREAM_PREFIX.length()), new Position(parts[0], Long.parseLong(parts[1]), Integer.parseInt(parts[2])));
            }
        });
        return saved;
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
        reportLag(session, current);
        List<Map<String, String>> flushes = dirty.takeFlushes();
        Batch batch = dirty.take(context.getProperty(MAX_FLOWFILES_PER_TRIGGER).asInteger());
        try {
            for (Map<String, String> flush : flushes) {
                session.transfer(session.putAllAttributes(session.create(), flush), REL_FLUSH);
            }
            if (batch.keys().isEmpty() && batch.checkpoint() == null) {
                session.commit();
                context.yield();
                return;
            }
            for (String key : batch.keys()) {
                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, RedisBytes.keyAttributes(RedisBytes.fromIdentity(key)));
                flowFile = session.putAttribute(flowFile, "redis.event.db", String.valueOf(current.setup.database()));
                session.transfer(flowFile, REL_KEY_CHANGED);
            }
            session.commit();
        } catch (RuntimeException e) {
            // The stream has moved past these; a rolled-back session would otherwise lose them.
            dirty.giveBack(batch.keys(), flushes);
            throw e;
        }
        if (batch.checkpoint() != null) {
            Map<String, String> state = new HashMap<>();
            batch.checkpoint().forEach((shard, position) -> state.put(STATE_STREAM_PREFIX + shard,
                    position.replId() + "|" + position.offset() + "|" + position.db()));
            try {
                context.getStateManager().setState(state, Scope.CLUSTER);
            } catch (IOException e) {
                getLogger().warn("Could not save the replication offsets; a restart will resume from earlier ones", e);
            }
        }
    }

    /**
     * Keeps one worker per master the pool names, starting one for a master that appears (a new
     * shard, or a replica a failover promoted) and stopping the one whose master no longer serves
     * slots. A stopped shard's position stays, for a promoted replica to continue from.
     */
    private void supervise(Run run) {
        while (run.running) {
            try {
                Map<String, ReplicationEndpoint> desired = run.setup.pool().replicationEndpoints();
                run.desired = desired;
                for (Shard shard : List.copyOf(run.shards.values())) {
                    if (!desired.containsKey(shard.id)) {
                        run.shards.remove(shard.id);
                        shard.stop();
                        run.dirty.retire(shard.id);
                        retiredShards.incrementAndGet();
                        getLogger().info("Master {} no longer serves slots; its stream is closed and its position kept for the master that takes over", shard.id);
                    }
                }
                for (String id : desired.keySet()) {
                    if (!run.shards.containsKey(id)) {
                        Shard shard = new Shard(id);
                        run.shards.put(id, shard);
                        Thread thread = new Thread(() -> stream(run, shard), "r2dfly-replication-" + getIdentifier() + "-" + id);
                        thread.setDaemon(true);
                        shard.thread = thread;
                        thread.start();
                    }
                }
            } catch (RuntimeException e) {
                getLogger().warn("Could not read the source's topology; checking again in {} ms", TOPOLOGY_CHECK_MS, e);
            } catch (InterruptedException e) {
                return;
            }
            try {
                Thread.sleep(TOPOLOGY_CHECK_MS);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /** One shard's replica connection, reconnecting until the shard is stopped. */
    private void stream(Run run, Shard shard) {
        Setup setup = run.setup;
        Dirty dirty = run.dirty;
        while (run.running && shard.running) {
            ReplicationStream opened = null;
            try {
                ReplicationEndpoint endpoint = run.desired.get(shard.id);
                if (endpoint == null) {
                    return;
                }
                Resume from = dirty.resumePoint(shard.id, () -> replicationIds(setup.pool(), shard.id));
                Position at = from == null ? null : from.position();
                opened = ReplicationStream.open(endpoint, at == null ? null : at.replId(), at == null ? -1 : at.offset(),
                        at == null ? -1 : at.db(), setup.connectTimeoutMs(), shard::connecting);
                if (opened.syncKind() == ReplicationStream.SyncKind.FULL) {
                    fullResyncs.incrementAndGet();
                    if (from == null) {
                        newShards.incrementAndGet();
                    }
                    getLogger().info("Full resync from {}:{} (shard {}, replication id {}, offset {}){}; scanning its keys",
                            endpoint.host(), endpoint.port(), shard.id, opened.replId(), opened.offset(),
                            from == null ? "" : ": it could not continue from offset " + at.offset() + " of shard " + from.shard());
                    dirty.fullResync(shard.id, opened.replId(), opened.offset());
                    startSnapshot(run, shard);
                } else {
                    partialResyncs.incrementAndGet();
                    if (!from.shard().equals(shard.id)) {
                        failoverResumes.incrementAndGet();
                        getLogger().info("Master {} took over from {}: its stream continues replication id {} after offset {}, no keys rescanned",
                                shard.id, from.shard(), at.replId(), at.offset());
                    } else {
                        getLogger().info("Partial resync from {}:{} continues replication id {} after offset {}",
                                endpoint.host(), endpoint.port(), at.replId(), at.offset());
                    }
                    dirty.continued(shard.id, from.shard(), opened.replId(), at.offset(), at.db());
                    // A failover during the old master's snapshot: the promoted replica holds the same keys.
                    if (!dirty.snapshotDone(shard.id)) {
                        startSnapshot(run, shard);
                    }
                }
                opened.run((db, argv, offsetAfter) -> onCommand(dirty, shard.id, setup.database(), db, argv, offsetAfter, setup.filter(), setup.keyTable()));
            } catch (IOException | RuntimeException e) {
                if (!run.running || !shard.running) {
                    return;
                }
                disconnects.incrementAndGet();
                getLogger().warn("Replication connection to shard {} lost; reconnecting in {} ms", shard.id, setup.backoffMs(), e);
            } finally {
                if (opened != null) {
                    dirty.advance(shard.id, opened.offset(), opened.db());
                }
                shard.disconnect();
            }
            try {
                Thread.sleep(setup.backoffMs());
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /** The master's current replication id and the one it inherited, if a failover promoted it. */
    private static Set<String> replicationIds(RedisConnectionPoolService pool, String shard) {
        String info = onShard(pool, shard, cmds -> cmds.info("replication"));
        Set<String> ids = new HashSet<>();
        for (String line : info.split("\r?\n")) {
            if (line.startsWith("master_replid:") || line.startsWith("master_replid2:")) {
                ids.add(line.substring(line.indexOf(':') + 1).trim());
            }
        }
        return ids;
    }

    private void onCommand(Dirty dirty, String shard, int database, int db, List<byte[]> argv, long offsetAfter, KeyFilter filter, CommandKeyTable keyTable) {
        if (argv.isEmpty()) {
            dirty.advance(shard, offsetAfter, db);
            return;
        }
        commands.incrementAndGet();
        String name = new String(argv.get(0), StandardCharsets.UTF_8).toLowerCase();
        if (name.equals("flushall") || (name.equals("swapdb") && touchesDb(argv, database))) {
            dirty.flush(shard, name, database, offsetAfter, db);
            return;
        }
        if (db != database) {
            byte[] arrived = keyArrivingFrom(name, argv, database);
            if (arrived == null) {
                otherDbCommands.incrementAndGet();
                dirty.advance(shard, offsetAfter, db);
            } else {
                markAllowed(dirty, shard, List.of(arrived), offsetAfter, db, filter);
            }
            return;
        }
        if (name.equals("flushdb")) {
            dirty.flush(shard, name, database, offsetAfter, db);
            return;
        }
        List<byte[]> keys;
        try {
            keys = keyTable.keys(argv);
        } catch (RuntimeException e) {
            if (!refusedBySource(e)) {
                // A timeout or a lost pool connection: the stream reconnects from before this
                // command and reads it again, rather than move past a change it could not place.
                throw e;
            }
            unknownKeyCommands.incrementAndGet();
            getLogger().warn("The source could not tell which keys {} touches; its change is not copied until the key changes again", name, e);
            dirty.advance(shard, offsetAfter, db);
            return;
        }
        markAllowed(dirty, shard, keys, offsetAfter, db, filter);
    }

    private void markAllowed(Dirty dirty, String shard, List<byte[]> keys, long offsetAfter, int db, KeyFilter filter) {
        List<String> kept = new ArrayList<>(keys.size());
        for (byte[] key : keys) {
            if (filter.allowsKey(key)) {
                kept.add(RedisBytes.identity(key));
            } else {
                prefixSkipped.incrementAndGet();
            }
        }
        dirty.mark(shard, kept, offsetAfter, db);
    }

    /**
     * The key a command run in another database puts into {@code database}: MOVE key db, and
     * COPY source destination DB db. The master propagates both under the database they ran in.
     */
    private static byte[] keyArrivingFrom(String name, List<byte[]> argv, int database) {
        String db = String.valueOf(database);
        if (name.equals("move") && argv.size() == 3 && db.equals(new String(argv.get(2), StandardCharsets.US_ASCII))) {
            return argv.get(1);
        }
        if (name.equals("copy")) {
            for (int i = 3; i + 1 < argv.size(); i++) {
                if (new String(argv.get(i), StandardCharsets.US_ASCII).equalsIgnoreCase("db")
                        && db.equals(new String(argv.get(i + 1), StandardCharsets.US_ASCII))) {
                    return argv.get(2);
                }
            }
        }
        return null;
    }

    /** The source answered GETKEYS with an error, which asking again would not change. */
    private static boolean refusedBySource(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof RedisCommandExecutionException) {
                return true;
            }
        }
        return false;
    }

    private static boolean touchesDb(List<byte[]> argv, int database) {
        String db = String.valueOf(database);
        return argv.size() > 2 && (db.equals(new String(argv.get(1), StandardCharsets.US_ASCII))
                || db.equals(new String(argv.get(2), StandardCharsets.US_ASCII)));
    }

    /** SCANs the shard's own master: after a full resync of one shard, only its keys need a copy. */
    private void startSnapshot(Run run, Shard shard) {
        Thread previous = shard.snapshotThread;
        if (previous != null) {
            previous.interrupt();
        }
        Setup setup = run.setup;
        Dirty dirty = run.dirty;
        long generation = dirty.generation(shard.id);
        Thread thread = new Thread(() -> {
            try {
                ScanCursor cursor = ScanCursor.INITIAL;
                while (!Thread.currentThread().isInterrupted() && run.running && shard.running) {
                    ScanCursor current = cursor;
                    KeyScanCursor<byte[]> page;
                    try {
                        page = onShard(setup.pool(), shard.id, cmds -> cmds.scan(current, ScanArgs.Builder.limit(setup.scanCount())));
                    } catch (RuntimeException e) {
                        // Retried from the same cursor: giving up would leave the rest of the shard
                        // uncopied with nothing to say so but the missing offset checkpoint.
                        getLogger().warn("Snapshot SCAN of shard {} failed; retrying in {} ms", shard.id, setup.backoffMs(), e);
                        Thread.sleep(setup.backoffMs());
                        continue;
                    }
                    List<String> keys = new ArrayList<>();
                    for (byte[] key : page.getKeys()) {
                        if (setup.filter().allowsKey(key)) {
                            keys.add(RedisBytes.identity(key));
                        }
                    }
                    if (!dirty.markSnapshot(shard.id, keys, generation)) {
                        return;
                    }
                    if (page.isFinished()) {
                        dirty.snapshotFinished(shard.id, generation);
                        getLogger().info("Snapshot scan of shard {} finished", shard.id);
                        return;
                    }
                    cursor = page;
                    while (dirty.size() > setup.maxPending() && !Thread.currentThread().isInterrupted()) {
                        Thread.sleep(50);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "r2dfly-snapshot-" + getIdentifier() + "-" + shard.id);
        thread.setDaemon(true);
        shard.snapshotThread = thread;
        thread.start();
    }

    private List<byte[]> getKeys(RedisConnectionPoolService pool, List<byte[]> argv) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).add("GETKEYS");
        argv.forEach(args::add);
        List<Object> reply = await(pool, cmds -> cmds.dispatch(CommandType.COMMAND, new ArrayOutput<>(ByteArrayCodec.INSTANCE), args));
        return reply.stream().map(k -> (byte[]) k).toList();
    }

    private static <T> T await(RedisConnectionPoolService pool, Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<T>> call) {
        return onShard(pool, RedisConnectionPoolService.STANDALONE_SHARD, call);
    }

    /** Runs a command on one shard's master: the pool's routing outside a cluster, the node's own connection in one. */
    @SuppressWarnings("unchecked")
    private static <T> T onShard(RedisConnectionPoolService pool, String shard,
                                 Function<RedisClusterAsyncCommands<byte[], byte[]>, RedisFuture<T>> call) {
        if (shard.equals(RedisConnectionPoolService.STANDALONE_SHARD)) {
            return pool.withConnection(cmds -> get(call.apply(cmds)));
        }
        return pool.withRawConnection(raw -> get(call.apply(
                ((StatefulRedisClusterConnection<byte[], byte[]>) raw).getConnection(shard).async())));
    }

    private static <T> T get(RedisFuture<T> future) {
        try {
            return future.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessException(e);
        } catch (Exception e) {
            throw new ProcessException(e);
        }
    }

    /**
     * How far the flow is behind the source, at most once a second: the bytes of the masters'
     * replication streams not yet read, the keys read but not yet emitted, the seconds since both
     * were last zero (or since this schedule started, if they never were), and whether they are
     * zero now with every master streaming and every snapshot finished.
     */
    private void reportLag(ProcessSession session, Run current) {
        long now = System.currentTimeMillis();
        if (now - current.lagCheckedAtMs < 1000) {
            return;
        }
        current.lagCheckedAtMs = now;
        Map<String, ReplicationEndpoint> desired = current.desired;
        long lagBytes = 0;
        // Read the masters first, so an offset that has reached its master's is caught up.
        Map<String, Long> masterOffsets = new HashMap<>();
        try {
            for (String shard : desired.keySet()) {
                Matcher m = MASTER_REPL_OFFSET.matcher(onShard(current.setup.pool(), shard, cmds -> cmds.info("replication")));
                if (!m.find()) {
                    gauges.set(session, "Replication Caught Up", 0);
                    return;
                }
                masterOffsets.put(shard, Long.parseLong(m.group(1)));
            }
        } catch (RuntimeException e) {
            getLogger().debug("Could not read the source's replication offsets", e);
            gauges.set(session, "Replication Caught Up", 0);
            return;
        }
        Progress progress = current.dirty.progress();
        boolean caughtUp = progress.pendingKeys() == 0;
        for (Map.Entry<String, Long> master : masterOffsets.entrySet()) {
            Position streamed = progress.streaming().get(master.getKey());
            long behind = streamed == null ? master.getValue() : Math.max(0, master.getValue() - streamed.offset());
            lagBytes += behind;
            caughtUp &= streamed != null && behind == 0 && !progress.snapshotting().contains(master.getKey());
        }
        if (caughtUp) {
            current.caughtUpAtMs = now;
        }
        // A zero gauge reads the same as one never reported, so being caught up is its own flag.
        gauges.set(session, "Replication Caught Up", caughtUp ? 1 : 0);
        gauges.set(session, "Replication Lag (bytes)", lagBytes);
        gauges.set(session, "Replication Lag (seconds)", (now - current.caughtUpAtMs) / 1000);
        gauges.set(session, "Replication Pending Keys", progress.pendingKeys());
        gauges.set(session, "Replication Streams", progress.streaming().size());
    }

    private void reportCounters(ProcessSession session) {
        drain(session, "Replication Full Resyncs", fullResyncs);
        drain(session, "Replication Partial Resyncs", partialResyncs);
        drain(session, "Replication Disconnects", disconnects);
        drain(session, "Replication Commands", commands);
        drain(session, "Replication Keys Skipped (Prefix Filter)", prefixSkipped);
        drain(session, "Replication Commands in Other Databases", otherDbCommands);
        drain(session, "Replication Commands With Unknown Keys", unknownKeyCommands);
        drain(session, "Replication Failover Resumes", failoverResumes);
        drain(session, "Replication New Shards", newShards);
        drain(session, "Replication Shards Retired", retiredShards);
    }

    private static void drain(ProcessSession session, String name, AtomicLong counter) {
        long value = counter.getAndSet(0);
        if (value != 0) {
            session.adjustCounter(name, value, false);
        }
    }

    record Setup(RedisConnectionPoolService pool, int database, KeyFilter filter, CommandKeyTable keyTable,
                 int backoffMs, int connectTimeoutMs, int scanCount, int maxPending) {
    }

    /**
     * One schedule's state. A thread that outlives onStopped's join (stuck in a connect timeout,
     * say) only ever reaches its own Run, never the next schedule's dirty set or sockets.
     */
    private static final class Run {
        final Setup setup;
        final Dirty dirty = new Dirty();
        final Map<String, Shard> shards = new ConcurrentHashMap<>();
        volatile Map<String, ReplicationEndpoint> desired = Map.of();
        volatile boolean running = true;
        volatile long lagCheckedAtMs;
        volatile long caughtUpAtMs = System.currentTimeMillis();
        volatile Thread supervisor;

        Run(Setup setup) {
            this.setup = setup;
        }

        void stop() throws InterruptedException {
            running = false;
            if (supervisor != null) {
                supervisor.interrupt();
                supervisor.join(10_000);
            }
            for (Shard shard : shards.values()) {
                shard.stop();
            }
        }
    }

    /** One master's replica connection and the threads that serve it. */
    private static final class Shard {
        final String id;
        volatile boolean running = true;
        volatile Thread thread;
        volatile Thread snapshotThread;
        private volatile Closeable connection;

        Shard(String id) {
            this.id = id;
        }

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
            for (Thread t : new Thread[]{thread, snapshotThread}) {
                if (t != null) {
                    t.interrupt();
                    t.join(10_000);
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

    /** Where one stream is: the replication history it follows, the offset reached, and the database selected there. */
    record Position(String replId, long offset, int db) {
    }

    /** The position a shard's next PSYNC asks to continue from, and the shard it belonged to. */
    record Resume(String shard, Position position) {
    }

    record Progress(Map<String, Position> streaming, Set<String> snapshotting, int pendingKeys) {
    }

    record Batch(List<String> keys, Map<String, Position> checkpoint) {
    }

    /**
     * The keys waiting to be emitted and where each shard's stream is, guarded by one lock that the
     * stream threads, the snapshot threads and onTrigger share. A drain takes the whole pending set
     * at once with every stream's position at that moment; those positions become saveable once the
     * drained set is fully emitted. A shard is saved only once its snapshot has finished.
     */
    private static final class Dirty {
        private LinkedHashSet<String> pending = new LinkedHashSet<>();
        private LinkedHashSet<String> draining = new LinkedHashSet<>();
        private final List<Map<String, String>> flushes = new ArrayList<>();
        private final Map<String, Stream> streams = new HashMap<>();
        private Map<String, Position> drainCheckpoint;
        private Map<String, Position> lastSaved = Map.of();

        private static final class Stream {
            Position position;
            boolean active;
            boolean snapshotDone;
            long generation;

            Stream(Position position, boolean snapshotDone) {
                this.position = position;
                this.snapshotDone = snapshotDone;
            }
        }

        synchronized void restore(Map<String, Position> saved) {
            saved.forEach((shard, position) -> streams.put(shard, new Stream(position, true)));
            lastSaved = Map.copyOf(saved);
        }

        /**
         * The shard's own position if it streamed before, otherwise the position of a stopped shard
         * whose replication history this master continues: a failover's promoted replica reports
         * its old master's id as master_replid2.
         */
        Resume resumePoint(String shard, java.util.function.Supplier<Set<String>> masterIds) {
            synchronized (this) {
                Stream own = streams.get(shard);
                if (own != null) {
                    return new Resume(shard, own.position);
                }
                if (streams.isEmpty()) {
                    return null;
                }
            }
            // The old master may still read as active: the client can see the promoted replica
            // before it marks the old master failed.
            Set<String> ids = masterIds.get();
            synchronized (this) {
                return streams.entrySet().stream()
                        .filter(e -> e.getValue().position != null && ids.contains(e.getValue().position.replId()))
                        .sorted(java.util.Comparator.comparing(e -> e.getValue().active))
                        .map(e -> new Resume(e.getKey(), e.getValue().position))
                        .findFirst().orElse(null);
            }
        }

        synchronized boolean snapshotDone(String shard) {
            return streams.get(shard).snapshotDone;
        }

        /** A new snapshot of this shard supersedes whatever an earlier one had not finished. */
        synchronized void fullResync(String shard, String replId, long offset) {
            Stream stream = streams.computeIfAbsent(shard, s -> new Stream(null, false));
            stream.position = new Position(replId, offset, 0);
            stream.active = true;
            stream.snapshotDone = false;
            stream.generation++;
            drainCheckpoint = null;
        }

        /** The stream picks up where {@code from} was; a different {@code from} is a failover, and its position moves here. */
        synchronized void continued(String shard, String from, String replId, long resumedAfter, int resumedDb) {
            Stream stream = streams.remove(from);
            stream.position = new Position(replId, resumedAfter, resumedDb);
            stream.active = true;
            streams.put(shard, stream);
        }

        synchronized void retire(String shard) {
            Stream stream = streams.get(shard);
            if (stream != null) {
                stream.active = false;
            }
        }

        synchronized long generation(String shard) {
            return streams.get(shard).generation;
        }

        synchronized void mark(String shard, List<String> keys, long offsetAfter, int atDb) {
            pending.addAll(keys);
            advance(shard, offsetAfter, atDb);
        }

        synchronized void advance(String shard, long offsetAfter, int atDb) {
            Stream stream = streams.get(shard);
            if (stream != null && stream.position != null) {
                stream.position = new Position(stream.position.replId(), offsetAfter, atDb);
            }
        }

        synchronized void flush(String shard, String command, int db, long offsetAfter, int atDb) {
            flushes.add(Map.of("redis.flush.command", command, "redis.event.db", String.valueOf(db)));
            advance(shard, offsetAfter, atDb);
        }

        /** False once a newer full resync of the shard has replaced the snapshot this generation belongs to. */
        synchronized boolean markSnapshot(String shard, List<String> keys, long forGeneration) {
            Stream stream = streams.get(shard);
            if (stream == null || stream.generation != forGeneration) {
                return false;
            }
            pending.addAll(keys);
            return true;
        }

        synchronized void snapshotFinished(String shard, long forGeneration) {
            Stream stream = streams.get(shard);
            if (stream != null && stream.generation == forGeneration) {
                stream.snapshotDone = true;
            }
        }

        synchronized int size() {
            return pending.size() + draining.size();
        }

        synchronized Progress progress() {
            Map<String, Position> streaming = new HashMap<>();
            Set<String> snapshotting = new HashSet<>();
            streams.forEach((shard, stream) -> {
                if (stream.active) {
                    streaming.put(shard, stream.position);
                    if (!stream.snapshotDone) {
                        snapshotting.add(shard);
                    }
                }
            });
            return new Progress(streaming, snapshotting, size());
        }

        synchronized List<Map<String, String>> takeFlushes() {
            List<Map<String, String>> taken = List.copyOf(flushes);
            flushes.clear();
            return taken;
        }

        synchronized Batch take(int max) {
            if (draining.isEmpty()) {
                draining = pending;
                pending = new LinkedHashSet<>();
                Map<String, Position> positions = new java.util.TreeMap<>();
                streams.forEach((shard, stream) -> {
                    if (stream.snapshotDone && stream.position != null) {
                        positions.put(shard, stream.position);
                    }
                });
                // The masters' PINGs keep moving idle offsets; saving them every trigger would only churn cluster state.
                drainCheckpoint = positions.equals(lastSaved) ? null : positions;
            }
            List<String> keys = new ArrayList<>(Math.min(max, draining.size()));
            Iterator<String> it = draining.iterator();
            while (it.hasNext() && keys.size() < max) {
                keys.add(it.next());
                it.remove();
            }
            Map<String, Position> checkpoint = null;
            if (draining.isEmpty() && drainCheckpoint != null) {
                checkpoint = drainCheckpoint;
                drainCheckpoint = null;
                lastSaved = checkpoint;
            }
            return new Batch(keys, checkpoint);
        }

        synchronized void giveBack(List<String> keys, List<Map<String, String>> takenFlushes) {
            LinkedHashSet<String> restored = new LinkedHashSet<>(keys);
            restored.addAll(draining);
            draining = restored;
            flushes.addAll(0, takenFlushes);
        }
    }
}
