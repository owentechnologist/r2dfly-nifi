package io.dragonfly.nifi.redis.processors;

import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.dragonfly.nifi.redis.util.RedisBytes;
import io.dragonfly.nifi.redis.util.ClusterTopologySnapshot;
import io.dragonfly.nifi.redis.util.CommandBuilder;
import io.dragonfly.nifi.redis.util.CounterGauge;
import io.dragonfly.nifi.redis.util.FailureReason;
import io.dragonfly.nifi.redis.util.Futures;
import io.dragonfly.nifi.redis.util.KeyFilter;
import io.dragonfly.nifi.redis.util.PartitionAssigner;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"redis", "migration", "scan", "source"})
@CapabilityDescription("Scans the source Redis keyspace, partitioned across concurrent tasks by hash-slot "
        + "range (cluster mode) or CRC16 modulo (standalone/sentinel), emitting one empty FlowFile per key "
        + "with type/TTL metadata attributes. Routes by data type for downstream processing by "
        + "RedisTypeDeserializer. Spec section 4.1.")
public class RedisScanReader extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .required(true)
            .identifiesControllerService(RedisConnectionPoolService.class)
            .build();

    public static final PropertyDescriptor MIGRATION_ID = new PropertyDescriptor.Builder()
            .name("migration-id")
            .displayName("Migration ID")
            .description("Unique identifier for this migration run, shared with the other migration "
                    + "processors, used to namespace cursor-checkpoint and partition-claim cache keys.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CURSOR_STATE_CACHE = new PropertyDescriptor.Builder()
            .name("cursor-state-cache")
            .displayName("Cursor State Cache")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor PARTITION_COUNT = new PropertyDescriptor.Builder()
            .name("partition-count")
            .displayName("Partition Count")
            .description("Total number of partitions; must match this processor's Concurrent Tasks setting. A cluster's "
                    + "masters are dealt out among the partitions, each scanned once on its own node. A standalone or "
                    + "Sentinel source is scanned once by partition 0 whatever this says: more scans of one keyspace "
                    + "only add source load, so parallelism goes in RedisTypeDeserializer's Concurrent Tasks instead.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor SCAN_COUNT = new PropertyDescriptor.Builder()
            .name("scan-count")
            .displayName("Scan Count")
            .description("COUNT hint per SCAN call.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("200")
            .build();

    public static final PropertyDescriptor KEY_PATTERN = new PropertyDescriptor.Builder()
            .name("key-pattern")
            .displayName("Key Pattern")
            .description("SCAN MATCH pattern.")
            .required(true)
            .defaultValue("*")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_TYPE_FILTER = new PropertyDescriptor.Builder()
            .name("key-type-filter")
            .displayName("Key Type Filter")
            .description("Comma-separated list of types to include: string,hash,list,set,zset,stream.")
            .required(true)
            .defaultValue("string,hash,list,set,zset,stream")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PREFIX_DENY_LIST = new PropertyDescriptor.Builder()
            .name("prefix-deny-list")
            .displayName("Prefix Deny List")
            .description("Comma-separated list of key prefixes to exclude from migration. A key is "
                    + "skipped if it starts with any of these prefixes. Leave unset to disable.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PREFIX_ONLY_LIST = new PropertyDescriptor.Builder()
            .name("prefix-only-list")
            .displayName("Prefix Only List")
            .description("Comma-separated list of key prefixes to exclusively migrate. If set, a key is "
                    + "skipped unless it starts with one of these prefixes. Applied after Prefix Deny "
                    + "List. Leave unset to migrate all prefixes.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor EMIT_TTL = new PropertyDescriptor.Builder()
            .name("emit-ttl")
            .displayName("Emit TTL")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor SKIP_VOLATILE_KEYS = new PropertyDescriptor.Builder()
            .name("skip-volatile-keys")
            .displayName("Skip Volatile Keys")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor VOLATILE_KEY_TTL_THRESHOLD_MS = new PropertyDescriptor.Builder()
            .name("volatile-key-ttl-threshold-ms")
            .displayName("Volatile Key TTL Threshold (ms)")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor DATABASE_INDEX = new PropertyDescriptor.Builder()
            .name("database-index-override")
            .displayName("Database Index")
            .description("Override the connection pool's default database index. Standalone/sentinel only.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLAIM_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("claim-timeout-ms")
            .displayName("Claim Timeout (ms)")
            .description("A partition whose claim has not been refreshed for this long belonged to a task that died "
                    + "(a killed NiFi node, say) and is taken over by the next task that looks for work. Needs a Cursor "
                    + "State Cache with compare-and-replace, such as MapCacheClientService; with any other cache a "
                    + "claim is never taken over.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("300000")
            .build();

    public static final PropertyDescriptor READ_ENCODING = new PropertyDescriptor.Builder()
            .name("read-encoding")
            .displayName("Read Encoding")
            .description("Read each key's OBJECT ENCODING into redis.encoding. RedisTypeDeserializer's RESTORE probe "
                    + "decides per type and encoding (a Redis 7.4 hash with field TTLs is the only listpackex encoding "
                    + "Dragonfly cannot restore), so turning this off sends such keys through the restore_rejected loop "
                    + "instead. Sources without OBJECT, such as some Dragonfly builds, simply leave it unset.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor MAX_SOURCE_LATENCY_MS = new PropertyDescriptor.Builder()
            .name("max-source-latency-ms")
            .displayName("Max Source Latency (ms)")
            .description("Pause between SCAN pages while a page (the SCAN and the reads that follow it) takes longer "
                    + "than this, growing the pause up to a second until the source recovers. 0 turns it off.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();

    public static final PropertyDescriptor MAX_SOURCE_OPS_PER_SEC = new PropertyDescriptor.Builder()
            .name("max-source-ops-per-sec")
            .displayName("Max Source Ops per Second")
            .description("Pause between SCAN pages while the source's INFO instantaneous_ops_per_sec, which counts "
                    + "its own clients as well as this scan, is above this. 0 turns it off.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();

    public static final Relationship REL_STRING = new Relationship.Builder().name("string").description("Key is of type STRING").build();
    public static final Relationship REL_HASH = new Relationship.Builder().name("hash").description("Key is of type HASH").build();
    public static final Relationship REL_LIST = new Relationship.Builder().name("list").description("Key is of type LIST").build();
    public static final Relationship REL_SET = new Relationship.Builder().name("set").description("Key is of type SET").build();
    public static final Relationship REL_ZSET = new Relationship.Builder().name("zset").description("Key is of type ZSET").build();
    public static final Relationship REL_STREAM = new Relationship.Builder().name("stream").description("Key is of type STREAM").build();
    public static final Relationship REL_UNKNOWN = new Relationship.Builder().name("unknown").description("Key type is unrecognised or a module type").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("SCAN or TYPE command failed for this key").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REDIS_CONNECTION_POOL, MIGRATION_ID, CURSOR_STATE_CACHE, PARTITION_COUNT, SCAN_COUNT, KEY_PATTERN,
            KEY_TYPE_FILTER, PREFIX_DENY_LIST, PREFIX_ONLY_LIST, EMIT_TTL, SKIP_VOLATILE_KEYS,
            VOLATILE_KEY_TTL_THRESHOLD_MS, DATABASE_INDEX, CLAIM_TIMEOUT_MS, READ_ENCODING, MAX_SOURCE_LATENCY_MS, MAX_SOURCE_OPS_PER_SEC);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_STRING, REL_HASH, REL_LIST, REL_SET, REL_ZSET, REL_STREAM, REL_UNKNOWN, REL_FAILURE);

    private final ThreadLocal<PartitionAssigner.Claim> assignedPartition = new ThreadLocal<>();
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
        RedisConnectionPoolService redisPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPoolService.class);
        DistributedMapCacheClient cache = context.getProperty(CURSOR_STATE_CACHE).asControllerService(DistributedMapCacheClient.class);
        String migrationId = context.getProperty(MIGRATION_ID).getValue();
        PartitionAssigner assigner = new PartitionAssigner(cache, migrationId);
        int partitionCount = context.getProperty(PARTITION_COUNT).asInteger();

        PartitionAssigner.Claim claim = getOrClaimPartition(assigner, partitionCount, context.getProperty(CLAIM_TIMEOUT_MS).asLong());
        if (claim == null) {
            getLogger().info("No partition available to claim for migration '{}' ({} partitions); yielding", migrationId, partitionCount);
            context.yield();
            return;
        }
        int partitionIndex = claim.partition();
        try {
            if (assigner.isPartitionComplete(partitionIndex)) {
                getLogger().info("Partition {} already marked complete for migration '{}'; yielding", partitionIndex, migrationId);
                // Free to claim another, such as one a dead node left behind.
                assignedPartition.remove();
                context.yield();
                return;
            }
        } catch (IOException e) {
            throw new ProcessException("Unable to read partition completion state", e);
        }
        getLogger().info("Scanning partition {} of {} for migration '{}'", partitionIndex, partitionCount, migrationId);

        int scanCount = context.getProperty(SCAN_COUNT).asInteger();
        String pattern = context.getProperty(KEY_PATTERN).getValue();
        KeyFilter filter = KeyFilter.parse(context.getProperty(KEY_TYPE_FILTER).getValue(),
                context.getProperty(PREFIX_DENY_LIST).getValue(), context.getProperty(PREFIX_ONLY_LIST).getValue());
        boolean emitTtl = context.getProperty(EMIT_TTL).asBoolean();
        boolean skipVolatile = context.getProperty(SKIP_VOLATILE_KEYS).asBoolean();
        long volatileThresholdMs = context.getProperty(VOLATILE_KEY_TTL_THRESHOLD_MS).isSet()
                ? context.getProperty(VOLATILE_KEY_TTL_THRESHOLD_MS).asLong() : Long.MIN_VALUE;
        Integer databaseIndex = context.getProperty(DATABASE_INDEX).isSet() ? context.getProperty(DATABASE_INDEX).asInteger() : null;
        boolean readEncoding = context.getProperty(READ_ENCODING).asBoolean();
        Throttle throttle = new Throttle(context.getProperty(MAX_SOURCE_LATENCY_MS).asLong(),
                context.getProperty(MAX_SOURCE_OPS_PER_SEC).asLong());

        Plan plan = plan(redisPool, assigner, partitionIndex, partitionCount);
        Place place = restorePlace(assigner, partitionIndex, plan.targets());
        getLogger().info("Scanning partition {} of {} for migration '{}': shards {} from {} (allowedTypes={}, pattern='{}')",
                partitionIndex, partitionCount, migrationId, plan.targets(), place, filter.allowedTypes(), pattern);

        Topology topology = new Topology(redisPool);
        while (place != null && !Thread.currentThread().isInterrupted()) {
            Place at = place;
            long startedAt = System.nanoTime();
            ScanPage page;
            Place next;
            if (at.target().equals(STANDALONE)) {
                page = redisPool.withConnectionAndRaw((cmds, raw) -> scanPage(cmds, cmds, ScanCursor.of(at.step()), scanCount, pattern,
                        filter, emitTtl, skipVolatile, volatileThresholdMs, readEncoding));
                next = page.cursorAfterPage.isFinished() ? null : new Place(STANDALONE, STANDALONE, page.cursorAfterPage.getCursor());
            } else {
                ClusterTopologySnapshot current = topology.current();
                Set<Integer> shardSlots = plan.baseline().mastersById().get(at.target()).slots();
                if (!at.rescanningSlots()) {
                    String node = scanNode(at.target(), shardSlots, current);
                    Place scanning = at;
                    if (!node.equals(at.node())) {
                        if (at.node() != null) {
                            session.adjustCounter("Scan Shards Restarted After Failover", 1, false);
                            getLogger().info("Shard {}: its slots moved from {} to {}, so its scan restarts on {}; other shards are unaffected",
                                    at.target(), at.node(), node, node);
                        }
                        scanning = new Place(at.target(), node, ScanCursor.INITIAL.getCursor());
                    }
                    Place from = scanning;
                    page = redisPool.withConnectionAndRaw((cmds, raw) -> scanPage(nodeCommands(raw, node, redisPool.prefersReplicas()), cmds,
                            ScanCursor.of(from.step()), scanCount, pattern, filter, emitTtl, skipVolatile, volatileThresholdMs, readEncoding));
                    next = page.cursorAfterPage.isFinished() ? new Place(at.target(), node, Place.SLOTS + 0)
                            : new Place(at.target(), node, page.cursorAfterPage.getCursor());
                } else {
                    List<Integer> sorted = shardSlots.stream().sorted().toList();
                    int index = at.slotIndex();
                    // A slot still migrating off the scanned node has some keys on the importing node already,
                    // past where that node's own scan may have gone, so it is read again like a moved slot.
                    Set<Integer> migrating = migratingSlots(redisPool, at.node());
                    while (index < sorted.size() && at.node().equals(ownerOf(sorted.get(index), current)) && !migrating.contains(sorted.get(index))) {
                        index++;
                    }
                    if (index == sorted.size()) {
                        page = null;
                        next = nextTarget(plan.targets(), at.target());
                    } else {
                        int slot = sorted.get(index);
                        page = rescanSlot(redisPool, topology, slot, scanCount, pattern, filter, emitTtl, skipVolatile, volatileThresholdMs, readEncoding);
                        session.adjustCounter("Scan Slots Rescanned", 1, false);
                        getLogger().info("Shard {}: slot {} moved off {} during or before its scan; read again on its new owner ({} keys)",
                                at.target(), slot, at.node(), page.entries.size());
                        next = new Place(at.target(), at.node(), Place.SLOTS + (index + 1));
                    }
                }
            }
            long pageMs = (System.nanoTime() - startedAt) / 1_000_000;
            if (page != null) {
                emit(session, page, partitionIndex, databaseIndex);
                gauges.set(session, "Source Latency (ms)", pageMs);
            }
            place = next;
            // Checked before the page leaves: a task that stalled past the claim timeout while another took
            // its partition over must neither emit the page nor move the new owner's cursor.
            boolean held;
            try {
                held = assigner.heartbeat(claim);
            } catch (IOException e) {
                throw new ProcessException("Unable to heartbeat the scan partition claim", e);
            }
            if (!held) {
                session.rollback();
                assignedPartition.remove();
                getLogger().warn("Partition {} of migration '{}' was taken over by another task while this one stalled; stopping here", partitionIndex, migrationId);
                return;
            }
            // The page's FlowFiles first: a cursor saved ahead of them would skip the page if the commit failed.
            // A crash between the two re-reads the page, which only copies its keys again.
            session.commit();
            try {
                assigner.checkpointCursor(partitionIndex, place == null ? "" : place.encode());
            } catch (IOException e) {
                throw new ProcessException("Unable to checkpoint SCAN cursor", e);
            }
            long pauseMs = throttle.pauseAfter(pageMs, () -> opsPerSec(redisPool));
            if (pauseMs > 0) {
                session.adjustCounter("Scan Throttle Pause (ms)", pauseMs, false);
                try {
                    Thread.sleep(pauseMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (place == null) {
            try {
                assigner.markPartitionComplete(partitionIndex);
            } catch (IOException e) {
                getLogger().warn("Failed to mark partition {} complete", partitionIndex, e);
            }
            // Clear so this thread, if NiFi's pool reuses it for a later invocation, claims a
            // fresh partition instead of forever re-returning this now-complete one and yielding.
            assignedPartition.remove();
        }
    }

    private void emit(ProcessSession session, ScanPage page, int partitionIndex, Integer databaseIndex) {
        for (ScanEntry entry : page.entries) {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, entry.attributes(partitionIndex, page.cursorAfterPage, databaseIndex));
            session.transfer(flowFile, relationshipFor(entry.type));
        }
        page.skippedByType.forEach((type, count) -> session.adjustCounter("Scan Keys Skipped by Type Filter (" + type + ")", count, false));
        session.adjustCounter("Keys Scanned", page.entries.size() + page.failures.size()
                + page.skippedByType.values().stream().mapToLong(Long::longValue).sum(), false);
        for (ScanEntry failed : page.failures) {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, failed.attributes(partitionIndex, page.cursorAfterPage, databaseIndex));
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /** The shard name of a standalone or Sentinel source, which has a single keyspace to scan. */
    private static final String STANDALONE = "-";

    /**
     * What this partition scans: shards of the baseline topology (a cluster's masters, with their
     * slots, when the scan began; recorded once and shared by every partition), dealt out
     * round-robin by node id. One SCAN of a standalone source covers everything, so partition 0
     * takes it all and the rest have nothing to do; their parallelism belongs downstream, in
     * RedisTypeDeserializer's concurrent tasks.
     */
    record Plan(ClusterTopologySnapshot baseline, List<String> targets) {
    }

    private static Plan plan(RedisConnectionPoolService pool, PartitionAssigner assigner, int partitionIndex, int partitionCount) {
        if (!pool.isClusterMode()) {
            return new Plan(null, partitionIndex == 0 ? List.of(STANDALONE) : List.of());
        }
        ClusterTopologySnapshot baseline;
        try {
            String proposed = pool.refreshedTopology().orElseThrow(() -> new ProcessException("Cluster topology unavailable")).encode();
            baseline = ClusterTopologySnapshot.decode(assigner.baselineTopology(proposed));
        } catch (IOException e) {
            throw new ProcessException("Unable to read the scan's baseline topology", e);
        }
        // Slot owners only: a node that briefly reads as a slotless master while the cluster is
        // still converging can be a replica holding a copy of another master's keys.
        List<String> masters = baseline.mastersById().values().stream().filter(m -> !m.slots().isEmpty())
                .map(ClusterTopologySnapshot.MasterNode::nodeId).sorted().toList();
        List<String> mine = new ArrayList<>();
        for (int i = partitionIndex; i < masters.size(); i += partitionCount) {
            mine.add(masters.get(i));
        }
        return new Plan(baseline, mine);
    }

    /**
     * Where a partition is: the baseline shard it works on, the node it reads that shard on, and
     * its step there, a SCAN cursor or "slots:index" while it reads the shard's slots that moved
     * off that node. Encoded "target|node|step".
     */
    record Place(String target, String node, String step) {
        static final String SLOTS = "slots:";

        boolean rescanningSlots() {
            return step.startsWith(SLOTS);
        }

        int slotIndex() {
            return Integer.parseInt(step.substring(SLOTS.length()));
        }

        String encode() {
            return target + "|" + node + "|" + step;
        }

        static Optional<Place> decode(String saved) {
            String[] parts = saved.split("\\|", -1);
            return parts.length == 3 ? Optional.of(new Place(parts[0], parts[1], parts[2])) : Optional.empty();
        }

        @Override
        public String toString() {
            return encode();
        }
    }

    private static Place restorePlace(PartitionAssigner assigner, int partitionIndex, List<String> targets) {
        if (targets.isEmpty()) {
            return null;
        }
        try {
            Optional<Place> saved = assigner.restoreCursor(partitionIndex).flatMap(Place::decode);
            if (saved.isPresent() && targets.contains(saved.get().target())) {
                return saved.get();
            }
        } catch (IOException e) {
            throw new ProcessException("Unable to restore SCAN cursor", e);
        }
        String first = targets.get(0);
        return new Place(first, first.equals(STANDALONE) ? STANDALONE : null, ScanCursor.INITIAL.getCursor());
    }

    private static Place nextTarget(List<String> targets, String done) {
        int index = targets.indexOf(done) + 1;
        return index < targets.size() ? new Place(targets.get(index), null, ScanCursor.INITIAL.getCursor()) : null;
    }

    /** The shard's own master while it still holds some of the shard's slots, otherwise the master holding most of them (after a failover, the promoted replica). */
    private static String scanNode(String target, Set<Integer> shardSlots, ClusterTopologySnapshot current) {
        ClusterTopologySnapshot.MasterNode own = current.mastersById().get(target);
        if (own != null && own.slots().stream().anyMatch(shardSlots::contains)) {
            return target;
        }
        return current.mastersById().values().stream()
                .max(java.util.Comparator.comparingLong(m -> m.slots().stream().filter(shardSlots::contains).count()))
                .filter(m -> m.slots().stream().anyMatch(shardSlots::contains))
                .map(ClusterTopologySnapshot.MasterNode::nodeId)
                .orElseThrow(() -> new ProcessException("No master serves the slots of shard " + target));
    }

    /** SCAN MATCH's glob (* ? [...] and \\ escapes), for keys read without SCAN. */
    static boolean globMatches(String glob, String key) {
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append('.');
                case '[' -> {
                    // Redis's [abc], [^a] and [a-z] read the same as a regex character class.
                    int close = glob.indexOf(']', i + 1);
                    if (close < 0) {
                        regex.append("\\[");
                    } else {
                        regex.append(glob, i, close + 1);
                        i = close;
                    }
                }
                case '\\' -> {
                    if (i + 1 < glob.length()) {
                        regex.append(java.util.regex.Pattern.quote(String.valueOf(glob.charAt(++i))));
                    }
                }
                default -> regex.append(java.util.regex.Pattern.quote(String.valueOf(c)));
            }
        }
        return key.matches("(?s)" + regex);
    }

    private static String ownerOf(int slot, ClusterTopologySnapshot current) {
        for (ClusterTopologySnapshot.MasterNode master : current.mastersById().values()) {
            if (master.slots().contains(slot)) {
                return master.nodeId();
            }
        }
        return null;
    }

    /** The cluster as the client sees it, asked afresh at most every two seconds. */
    private static final class Topology {
        private final RedisConnectionPoolService pool;
        private ClusterTopologySnapshot snapshot;
        private long refreshedAtMs;

        Topology(RedisConnectionPoolService pool) {
            this.pool = pool;
        }

        ClusterTopologySnapshot current() {
            long now = System.currentTimeMillis();
            if (snapshot == null || now - refreshedAtMs >= 2000) {
                snapshot = pool.refreshedTopology().orElseThrow(() -> new ProcessException("Cluster topology unavailable"));
                refreshedAtMs = now;
            }
            return snapshot;
        }

        ClusterTopologySnapshot refreshed() {
            refreshedAtMs = 0;
            return current();
        }
    }

    private static final Pattern MIGRATING_SLOT = Pattern.compile("\\[(\\d+)->-");

    /** The slots {@code node} is migrating to another node, from its own line of CLUSTER NODES. */
    private static Set<Integer> migratingSlots(RedisConnectionPoolService pool, String node) {
        String nodes = pool.withConnectionAndRaw((cmds, raw) -> {
            try {
                return nodeCommands(raw, node, false).clusterNodes().get();
            } catch (Exception e) {
                throw new ProcessException("Reading CLUSTER NODES from " + node + " failed", e);
            }
        });
        Set<Integer> slots = new java.util.HashSet<>();
        for (String line : nodes.split("\n")) {
            if (line.contains("myself")) {
                Matcher m = MIGRATING_SLOT.matcher(line);
                while (m.find()) {
                    slots.add(Integer.parseInt(m.group(1)));
                }
            }
        }
        return slots;
    }

    /**
     * Every key of one slot, read on the slot's current owner once no migration is moving it: a
     * slot mid-migration has some keys on each side. Returns a page with no SCAN cursor of its own.
     */
    private ScanPage rescanSlot(RedisConnectionPoolService pool, Topology topology, int slot, int scanCount, String pattern, KeyFilter filter,
                                boolean emitTtl, boolean skipVolatile, long volatileThresholdMs, boolean readEncoding) {
        long deadline = System.currentTimeMillis() + 60_000;
        while (true) {
            String owner = ownerOf(slot, topology.refreshed());
            if (owner == null) {
                throw new ProcessException("Slot " + slot + " has no owner");
            }
            ScanPage page = pool.withConnectionAndRaw((cmds, raw) -> {
                RedisClusterAsyncCommands<byte[], byte[]> node = nodeCommands(raw, owner, false);
                try {
                    if (node.clusterNodes().get().matches("(?s).*\\[" + slot + "-[<>]-.*")) {
                        return null;
                    }
                    long count = node.clusterCountKeysInSlot(slot).get();
                    List<byte[]> keys = count == 0 ? List.of() : node.clusterGetKeysInSlot(slot, (int) count).get();
                    List<byte[]> matching = pattern == null || pattern.equals("*") ? keys
                            : keys.stream().filter(k -> globMatches(RedisBytes.identity(pattern.getBytes(StandardCharsets.UTF_8)), RedisBytes.identity(k))).toList();
                    return describe(cmds, matching, ScanCursor.of("slot:" + slot), filter, emitTtl, skipVolatile, volatileThresholdMs, readEncoding);
                } catch (ProcessException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ProcessException("Reading the keys of slot " + slot + " failed", e);
                }
            });
            if (page != null) {
                return page;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new ProcessException("Slot " + slot + " is still being migrated; its keys are read once it settles");
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessException(e);
            }
        }
    }

    /** With a replica-preferring pool, a replica of the master scans instead, if it has one. */
    @SuppressWarnings("unchecked")
    private static RedisClusterAsyncCommands<byte[], byte[]> nodeCommands(io.lettuce.core.api.StatefulConnection<byte[], byte[]> raw,
                                                                           String masterId, boolean preferReplica) {
        var cluster = (io.lettuce.core.cluster.api.StatefulRedisClusterConnection<byte[], byte[]>) raw;
        if (preferReplica) {
            for (io.lettuce.core.cluster.models.partitions.RedisClusterNode node : cluster.getPartitions()) {
                if (masterId.equals(node.getSlaveOf()) && node.isConnected()) {
                    return cluster.getConnection(node.getNodeId(), io.lettuce.core.protocol.ConnectionIntent.READ).async();
                }
            }
        }
        return cluster.getConnection(masterId).async();
    }

    private static long opsPerSec(RedisConnectionPoolService pool) {
        String stats = pool.withConnection(cmds -> {
            try {
                return cmds.info("stats").get();
            } catch (Exception e) {
                throw new ProcessException("INFO stats failed", e);
            }
        });
        for (String line : stats.split("\r?\n")) {
            if (line.startsWith("instantaneous_ops_per_sec:")) {
                return Long.parseLong(line.substring("instantaneous_ops_per_sec:".length()).trim());
            }
        }
        return 0;
    }

    /**
     * Waits between pages while the source is busier than allowed: the pause doubles (up to a
     * second) after each page that ran over a limit and halves after each that did not, so the
     * scan settles at the pace the source can take. INFO stats is read at most once a second.
     */
    static final class Throttle {
        private final long maxLatencyMs;
        private final long maxOpsPerSec;
        private long pauseMs;
        private long opsReadAtMs;
        private long lastOps;

        Throttle(long maxLatencyMs, long maxOpsPerSec) {
            this.maxLatencyMs = maxLatencyMs;
            this.maxOpsPerSec = maxOpsPerSec;
        }

        long pauseAfter(long pageMs, java.util.function.LongSupplier opsPerSec) {
            boolean over = maxLatencyMs > 0 && pageMs > maxLatencyMs;
            if (maxOpsPerSec > 0) {
                long now = System.currentTimeMillis();
                if (now - opsReadAtMs >= 1000) {
                    lastOps = opsPerSec.getAsLong();
                    opsReadAtMs = now;
                }
                over |= lastOps > maxOpsPerSec;
            }
            pauseMs = over ? Math.min(1000, Math.max(10, pauseMs * 2)) : pauseMs / 2;
            return pauseMs;
        }
    }

    /** The partition this thread holds, or a newly claimed one; null when none is free. */
    private PartitionAssigner.Claim getOrClaimPartition(PartitionAssigner assigner, int partitionCount, long claimTimeoutMs) {
        PartitionAssigner.Claim held = assignedPartition.get();
        if (held != null) {
            return held;
        }
        try {
            Optional<PartitionAssigner.Claim> claimed = assigner.claimPartition(partitionCount, claimTimeoutMs);
            if (claimed.isEmpty()) {
                return null;
            }
            assignedPartition.set(claimed.get());
            return claimed.get();
        } catch (IOException e) {
            throw new ProcessException("Unable to claim a scan partition", e);
        }
    }

    private Relationship relationshipFor(String type) {
        return switch (type) {
            case "string" -> REL_STRING;
            case "hash" -> REL_HASH;
            case "list" -> REL_LIST;
            case "set" -> REL_SET;
            case "zset" -> REL_ZSET;
            case "stream" -> REL_STREAM;
            default -> REL_UNKNOWN;
        };
    }

    /** {@code scanCmds} SCANs (a single node in cluster mode); {@code cmds} reads each key, following cluster redirects. */
    private static ScanPage scanPage(
            RedisClusterAsyncCommands<byte[], byte[]> scanCmds, RedisClusterAsyncCommands<byte[], byte[]> cmds, ScanCursor cursor,
            int scanCount, String pattern, KeyFilter filter, boolean emitTtl, boolean skipVolatile, long volatileThresholdMs,
            boolean readEncoding) {
        ScanArgs scanArgs = new ScanArgs().match(pattern).limit(scanCount);
        KeyScanCursor<byte[]> result;
        try {
            result = scanCmds.scan(cursor, scanArgs).get();
        } catch (Exception e) {
            throw new ProcessException("SCAN failed", e);
        }

        return describe(cmds, result.getKeys(), result, filter, emitTtl, skipVolatile, volatileThresholdMs, readEncoding);
    }

    /** The keys the filter allows, with their type, TTL and encoding read through {@code cmds}. */
    private static ScanPage describe(RedisClusterAsyncCommands<byte[], byte[]> cmds, List<byte[]> keys, ScanCursor cursorAfter, KeyFilter filter,
                                     boolean emitTtl, boolean skipVolatile, long volatileThresholdMs, boolean readEncoding) {
        List<byte[]> candidateKeys = new ArrayList<>();
        for (byte[] key : keys) {
            if (!filter.allowsKey(key)) {
                continue;
            }
            candidateKeys.add(key);
        }

        List<RedisFuture<String>> typeFutures = new ArrayList<>();
        for (byte[] key : candidateKeys) {
            typeFutures.add(cmds.type(key));
        }
        Futures.awaitAll(typeFutures.toArray(new RedisFuture<?>[0]), 60_000);

        List<ScanEntry> entries = new ArrayList<>();
        List<ScanEntry> failures = new ArrayList<>();
        java.util.Map<String, Long> skippedByType = new java.util.TreeMap<>();
        List<byte[]> ttlKeys = new ArrayList<>();
        List<String> ttlTypes = new ArrayList<>();
        List<RedisFuture<Long>> ttlFutures = new ArrayList<>();
        List<RedisFuture<String>> encodingFutures = new ArrayList<>();

        for (int i = 0; i < candidateKeys.size(); i++) {
            byte[] key = candidateKeys.get(i);
            String type;
            try {
                type = typeFutures.get(i).get();
            } catch (Exception e) {
                failures.add(new ScanEntry(key, "unknown", -2, null, "TYPE on the source failed: " + FailureReason.describe(e)));
                continue;
            }
            if (!filter.allowsType(type)) {
                skippedByType.merge(type, 1L, Long::sum);
                continue;
            }
            ttlKeys.add(key);
            ttlTypes.add(type);
            ttlFutures.add(emitTtl ? cmds.pttl(key) : null);
            encodingFutures.add(readEncoding ? cmds.objectEncoding(key) : null);
        }
        if (emitTtl) {
            Futures.awaitAll(ttlFutures.stream().filter(java.util.Objects::nonNull).toArray(RedisFuture<?>[]::new), 60_000);
        }
        Futures.awaitAll(encodingFutures.stream().filter(java.util.Objects::nonNull).toArray(RedisFuture<?>[]::new), 60_000);

        for (int i = 0; i < ttlKeys.size(); i++) {
            long ttlMs = -1;
            if (emitTtl) {
                try {
                    ttlMs = ttlFutures.get(i).get();
                } catch (Exception e) {
                    failures.add(new ScanEntry(ttlKeys.get(i), ttlTypes.get(i), -2, null, "PTTL on the source failed: " + FailureReason.describe(e)));
                    continue;
                }
            }
            if (skipVolatile && ttlMs >= 0 && ttlMs < volatileThresholdMs) {
                continue;
            }
            String encoding = null;
            try {
                encoding = encodingFutures.get(i) == null ? null : encodingFutures.get(i).get();
            } catch (Exception ignored) {
                // encoding is informational only
            }
            entries.add(new ScanEntry(ttlKeys.get(i), ttlTypes.get(i), ttlMs, encoding, null));
        }

        return new ScanPage(entries, failures, cursorAfter, skippedByType);
    }

    /** skippedByType: keys the type filter left out, which otherwise vanish without a trace. */
    private record ScanPage(List<ScanEntry> entries, List<ScanEntry> failures, ScanCursor cursorAfterPage,
                            java.util.Map<String, Long> skippedByType) {
    }

    private record ScanEntry(byte[] key, String type, long ttlMs, String encoding, String failure) {
        java.util.Map<String, String> attributes(int partitionIndex, ScanCursor cursor, Integer databaseIndex) {
            java.util.Map<String, String> attrs = new java.util.HashMap<>();
            attrs.putAll(RedisBytes.keyAttributes(key));
            attrs.put("redis.type", type);
            attrs.put("redis.ttl.ms", String.valueOf(ttlMs));
            attrs.put(CommandBuilder.TTL_READ_AT, String.valueOf(System.currentTimeMillis()));
            attrs.put("redis.partition.index", String.valueOf(partitionIndex));
            attrs.put("redis.scan.cursor", cursor.getCursor());
            if (databaseIndex != null) {
                attrs.put("redis.source.db", String.valueOf(databaseIndex));
            }
            if (encoding != null) {
                attrs.put("redis.encoding", encoding);
            }
            if (failure != null) {
                attrs.put(FailureReason.ATTRIBUTE, failure);
            }
            return attrs;
        }
    }
}
