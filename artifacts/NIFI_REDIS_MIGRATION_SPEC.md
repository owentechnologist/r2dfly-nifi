# NiFi-Based Redis → Dragonfly Migration Tool

## Implementation Specification for Coding Agent

**Document version:** 1.0  
**Target platform:** Apache NiFi 2.x ([https://nifi.apache.org/](https://nifi.apache.org/))  
**Source:** Any Redis-compatible datastore (Redis OSS, Redis Stack, Valkey, ElastiCache)  
**Target:** DragonflyDB (standalone or Dragonfly Cloud)  
**Language:** Java 17+ (NAR-packaged custom processors)  
**Build system:** Maven with NiFi NAR plugin

---

## 1\. Rationale and Design Philosophy

### 1.1 Why NiFi

Apache NiFi is a dataflow automation platform designed to reliably move and transform data between systems at scale. Its core architecture — directed graphs of processors connected by back-pressured queues — provides properties that bespoke migration scripts and tools like RedisShake or RIOT cannot deliver natively:

- **Provenance:** Every FlowFile carries an immutable, append-only audit trail of every processor that touched it, what changed, and when. For data migration, this means a complete lineage record of every key moved from source to target.  
- **Back-pressure:** Connection queues between processors enforce configurable size and age limits. If the writer falls behind the scanner, back-pressure automatically slows ingestion — no manual throttling flags, no OOM risk in the migration coordinator.  
- **Reliability:** FlowFiles that fail to process are not silently dropped. They route to a `failure` relationship, where they can be queued, retried, or routed to a dead-letter destination. Every failure is visible, attributable, and recoverable.  
- **Security:** NiFi provides TLS on all connections, credential management via its protected parameter context system, and role-based access control on all flow operations. Migration credentials never appear in plaintext configuration files or shell history.  
- **Parallelism:** NiFi's concurrent task model and clustering architecture allow the same processor to run across multiple threads on a single node, or across multiple nodes in a cluster — partitioning the source keyspace and parallelising both scan and write phases without external orchestration.

### 1.2 The Core Argument

> **Data worth replicating is data worth protecting.**

A migration is not a bulk copy operation — it is a trust transfer. Keys moved from Redis to Dragonfly carry business state: user sessions, ML features, rate limits, queued jobs, leaderboards, threat intelligence data. If a key is silently dropped because a network hiccup caused a write failure, or a type mismatch was swallowed by a bare `try/catch`, the downstream consequence may be a user logged out, a recommendation degraded, or a rate limit bypassed.

NiFi's design encodes this principle architecturally. There is no "fire and forget." Every FlowFile either reaches a terminal relationship (`success`, `failure`) with a recorded outcome, or it remains in a queue, visible and retrievable. The provenance store provides a queryable record of the full migration that can be used for post-migration validation, audit, and debugging.

This specification therefore rejects the common pattern of SCAN → DUMP → RESTORE pipelines that silently skip incompatible types. Every key is tracked. Every failure is surfaced. Every write is confirmed.

---

## 2\. Architecture Overview

### 2.1 Pipeline Topology

The migration consists of two parallel pipelines: a **Scan Phase** (full keyspace copy) and a **Live Phase** (incremental change capture). Both write to the same target via a shared batching write layer.

```
╔══════════════════════════════════════════════════════════════════╗
║  SCAN PHASE — Full keyspace copy, partitioned across N threads   ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  [RedisScanReader]                                               ║
║   Partition 0/N ──┬──[string]──┐                                 ║
║   Partition 1/N   ├──[hash]────┤                                 ║
║   Partition 2/N   ├──[list]────┼──→ [RedisTypeDeserializer]      ║
║       ...         ├──[set]─────┤       │                         ║
║   Partition N/N   ├──[zset]────┤       ▼                         ║
║                   ├──[stream]──┘  [RedisBatchWriter]             ║
║                   └──[unknown]──→ [failure queue]   │            ║
║                                                     ▼            ║
╠═════════════════════════════════════════════════╦═══════════════╣
║  LIVE PHASE — Incremental change capture (KSN)  ║   Dragonfly   ║
╠═════════════════════════════════════════════════╣   Target      ║
║                                                 ║               ║
║  [RedisKeyspaceEventConsumer]                   ║               ║
║       │                                         ║               ║
║       └──[key_changed]──→ [RedisSingleKeyFetch] ║               ║
║                               │                 ║               ║
║                               └──→ [RedisBatchWriter]           ║
║                                                 ╚═══════════════╣
╚══════════════════════════════════════════════════════════════════╝
```

### 2.2 Component Inventory

| Component | Type | Purpose |
| :---- | :---- | :---- |
| `RedisConnectionPoolService` | Controller Service | Connection pooling for source Redis |
| `DragonflyConnectionPoolService` | Controller Service | Connection pooling for Dragonfly target |
| `RedisScanReader` | Source Processor | Partitioned SCAN with type detection and TTL preservation |
| `RedisTypeDeserializer` | Transform Processor | Reads key content by type into structured FlowFile |
| `RedisBatchWriter` | Sink Processor | Batches FlowFiles into pipelined write commands to Dragonfly |
| `RedisKeyspaceEventConsumer` | Source Processor | Subscribes to keyspace notifications for live change capture |
| `RedisSingleKeyFetch` | Transform Processor | Fetches current value of a single key by name |
| `MigrationValidationProcessor` | Validation Processor | Spot-checks key existence and value equivalence post-migration |

---

## 3\. Controller Services

### 3.1 `RedisConnectionPoolService`

Extend NiFi's existing `RedisConnectionPoolService` (bundle: `nifi-redis-nar`). If extension is not possible, implement a new service wrapping Lettuce's `RedisClient` / `RedisClusterClient`.

**Required properties:**

| Property | Type | Description |
| :---- | :---- | :---- |
| `Connection Mode` | Enum: `STANDALONE`, `SENTINEL`, `CLUSTER` | Redis topology mode |
| `Connection String` | String (sensitive) | `redis://[:password@]host:port[/db]` or comma-separated for cluster/sentinel |
| `TLS Enabled` | Boolean | Enable TLS — default `true` |
| `TLS Trust Store Path` | String | Path to JKS/PKCS12 trust store |
| `TLS Trust Store Password` | String (sensitive) | Trust store password |
| `Connection Timeout (ms)` | Integer | Default `5000` |
| `Command Timeout (ms)` | Integer | Default `10000` |
| `Max Pool Size` | Integer | Lettuce connection pool size per NiFi thread — default `8` |
| `Database Index` | Integer | `SELECT` index for standalone mode — default `0`, ignored for cluster |

**Implementation notes:**

- Use Lettuce `StatefulRedisConnection` with `AsyncCommands` throughout — never use blocking sync commands on a NiFi thread.  
- In cluster mode, use `RedisClusterClient` with topology refresh enabled (`ClusterTopologyRefreshOptions.builder().enableAllAdaptiveRefreshTriggers()`).  
- Expose a `withConnection(Function<StatefulRedisConnection, T>)` method for processor use.

### 3.2 `DragonflyConnectionPoolService`

Identical interface to `RedisConnectionPoolService`. Dragonfly speaks the Redis protocol, so the same Lettuce client is used. Expose as a separate controller service so source and target credentials are managed independently and provenance distinguishes source vs target connections.

---

## 4\. Processor Specifications

### 4.1 `RedisScanReader`

**Purpose:** Scans the source Redis keyspace, partitioned by cursor range, emitting one FlowFile per key with metadata attributes. Routes FlowFiles by data type to enable type-specific downstream processing.

**Bundle:** `nifi-redis-migration-nar`

**Scheduling:** Designed to run with `N` concurrent tasks (one per partition). Set `Concurrent Tasks` to the desired partition count in the NiFi processor configuration. Each task claims an exclusive partition using the partition assignment mechanism described below.

#### 4.1.1 Partitioning Strategy

Redis's `SCAN` command does not support range-based partitioning natively. Implement partitioning using **hash-slot range assignment** (applicable to both cluster and standalone):

- Divide the 16,384 Redis hash slots into `N` equal ranges, where `N` \= configured partition count.  
- Each processor task is assigned a partition index via a distributed atomic counter stored in NiFi's `DistributedMapCacheClientService`.  
- For **cluster mode:** route SCAN commands to the shard owning the assigned slot range using Lettuce's `RedisAdvancedClusterCommands.scan()` with node-specific connections.  
- For **standalone mode:** use a key hash modulo filter: emit a FlowFile for key `k` only if `CRC16(k) % N == partitionIndex`. This is less efficient (all keys are scanned by all partitions) but correct — add a `--scan-sample-rate` optimization that uses `SCAN COUNT` tuning to reduce overhead.  
- Partition assignment and completion tracking must be idempotent — if a NiFi node restarts mid-migration, it re-claims its partition and resumes from the last committed cursor position (see section 4.1.4).

**Required properties:**

| Property | Type | Description |
| :---- | :---- | :---- |
| `Redis Connection Pool` | Controller Service | Source `RedisConnectionPoolService` |
| `Partition Count` | Integer | Total number of partitions — must match `Concurrent Tasks` |
| `Scan Count` | Integer | `COUNT` hint per SCAN call — default `200` |
| `Key Pattern` | String | MATCH pattern — default `*` |
| `Key Type Filter` | Multi-select Enum | Limit to specific types — default all |
| `Database Index` | Integer | Override pool default — standalone only |
| `Cursor State Cache` | Controller Service | `DistributedMapCacheClientService` for cursor checkpointing |
| `Emit TTL` | Boolean | Fetch and attach TTL per key — default `true` |
| `Skip Volatile Keys` | Boolean | Skip keys with TTL \< configured threshold — default `false` |
| `Volatile Key TTL Threshold (ms)` | Integer | Keys with TTL below this are skipped if `Skip Volatile Keys` \= true |

**Relationships:**

| Relationship | Description |
| :---- | :---- |
| `string` | Key is of type STRING |
| `hash` | Key is of type HASH |
| `list` | Key is of type LIST |
| `set` | Key is of type SET |
| `zset` | Key is of type ZSET (sorted set) |
| `stream` | Key is of type STREAM |
| `unknown` | Key type is unrecognised or module type — route to failure handling |
| `failure` | SCAN or TYPE command failed for this key |

**FlowFile attributes emitted:**

| Attribute | Value |
| :---- | :---- |
| `redis.key` | Key name (UTF-8) |
| `redis.type` | Type string: `string`, `hash`, `list`, `set`, `zset`, `stream` |
| `redis.ttl.ms` | Remaining TTL in milliseconds; `-1` if no expiry; `-2` if key missing |
| `redis.partition.index` | Partition index that produced this FlowFile |
| `redis.scan.cursor` | SCAN cursor value at time of emission (for checkpoint) |
| `redis.source.db` | Database index on source |
| `redis.encoding` | `OBJECT ENCODING` value (informational) |

**FlowFile content:** Empty at emission from `RedisScanReader`. Content is populated by `RedisTypeDeserializer`.

#### 4.1.2 SCAN Loop Implementation

```
onTrigger():
  partitionIndex = claimPartition(distributedCache, processorId)
  cursor = restoreCursor(distributedCache, partitionIndex) ?? ScanCursor.INITIAL

  loop:
    result = redisConn.async().scan(cursor, ScanArgs.count(scanCount).match(pattern)).get()
    for key in result.keys:
      type = redisConn.async().type(key).get()
      if typeFilter.excludes(type): continue
      ttl = emitTTL ? redisConn.async().pttl(key).get() : -1
      ff = session.create()
      ff = session.putAllAttributes(ff, buildAttributes(key, type, ttl, partitionIndex, cursor))
      session.transfer(ff, toRelationship(type))

    checkpointCursor(distributedCache, partitionIndex, result.cursor)
    session.commit()
    cursor = result.cursor
    if cursor.isFinished(): break

  markPartitionComplete(distributedCache, partitionIndex)
```

**Critical:** Call `session.commit()` after each SCAN page, not at the end of the full scan. This ensures partial progress is preserved if the processor is stopped or the node restarts.

#### 4.1.3 Cursor Checkpointing

Use `DistributedMapCacheClientService` with keys of the form:

```
redis.migration.cursor.<migrationId>.<partitionIndex>  →  <cursorValue>
redis.migration.partition.<migrationId>.claimed         →  Set<partitionIndex>
redis.migration.partition.<migrationId>.complete        →  Set<partitionIndex>
```

`migrationId` is a UUID set as a processor property, allowing multiple independent migrations to coexist in the same NiFi instance.

---

### 4.2 `RedisTypeDeserializer`

**Purpose:** Reads the full value of a key from the source Redis, encoding it into the FlowFile content as a structured JSON payload appropriate for the key's type. Operates on FlowFiles emitted by `RedisScanReader`.

**Bundle:** `nifi-redis-migration-nar`

**Scheduling:** Run with the same `Concurrent Tasks` as `RedisScanReader`. One task per incoming FlowFile.

**Required properties:**

| Property | Type | Description |
| :---- | :---- | :---- |
| `Redis Connection Pool` | Controller Service | Source `RedisConnectionPoolService` |
| `Hash Field Batch Size` | Integer | Max fields fetched per `HGETALL` call — default `10000` |
| `List Chunk Size` | Integer | Max elements per `LRANGE` call — default `5000` |
| `Stream Read Count` | Integer | Max entries per `XRANGE` call — default `1000` |
| `Include Consumer Groups` | Boolean | Fetch and serialise consumer group state for streams — default `true` |

**FlowFile content written** (JSON, one object per FlowFile):

```json
{
  "key": "user:session:abc123",
  "type": "hash",
  "ttl_ms": 86400000,
  "encoding": "ziplist",
  "value": {
    "userId": "42",
    "token": "xyz",
    "created": "1720000000"
  }
}
```

For `list`, `value` is an ordered JSON array. For `set`, an unordered JSON array. For `zset`, an array of `{"member": "x", "score": 1.5}` objects. For `stream`, an array of `{"id": "1234-0", "fields": {...}}` entries plus an optional `"consumer_groups"` array.

**Implementation notes:**

- Use pipelined async Lettuce commands to batch the `GET`/`HGETALL`/`LRANGE`/`SMEMBERS`/`ZRANGEBYRANK`/`XRANGE` call with any follow-up pagination into as few round trips as possible.  
- For large hashes and lists that exceed the configured chunk size, emit multiple FlowFiles with a `redis.chunk.index` attribute and `redis.chunk.total` count. `RedisBatchWriter` must re-assemble these before writing.  
- For `stream` type with consumer groups: call `XINFO GROUPS key` → for each group call `XINFO CONSUMERS key group` and `XPENDING key group - + MAX`. Write the full PEL state into the JSON payload.

**Relationships:**

| Relationship | Description |
| :---- | :---- |
| `success` | Value read and serialised successfully |
| `key_missing` | Key expired between SCAN and fetch — route to skip or log |
| `module_type` | Key is a module type (ReJSON-RL, TopK, etc.) — route to specialised handling |
| `failure` | Read failed — route to retry queue |

---

### 4.3 `RedisBatchWriter`

**Purpose:** Accepts FlowFiles from `RedisTypeDeserializer`, accumulates them into a batch, and issues pipelined write commands to Dragonfly. This is the performance-critical component — all writes to the target are issued through this processor's pipeline, never one command at a time.

**Bundle:** `nifi-redis-migration-nar`

**Scheduling:** Run with `Concurrent Tasks` equal to the desired write parallelism. Each task independently accumulates and flushes its own batch.

**Required properties:**

| Property | Type | Description |
| :---- | :---- | :---- |
| `Dragonfly Connection Pool` | Controller Service | Target `DragonflyConnectionPoolService` |
| `Batch Size` | Integer | Number of FlowFiles to accumulate before flushing — default `500` |
| `Batch Timeout (ms)` | Integer | Flush batch after this time even if not full — default `1000` |
| `Max Pipeline Depth` | Integer | Max outstanding async commands before awaiting completions — default `2000` |
| `TTL Strategy` | Enum: `PRESERVE`, `STRIP`, `RESET` | How to handle TTLs: preserve from source, strip all, or reset to a fixed offset |
| `TTL Reset Offset (ms)` | Integer | Added to remaining TTL when strategy is `RESET` |
| `Key Prefix` | String | Optional prefix prepended to all target keys — default empty |
| `Key Prefix Separator` | String | Separator between prefix and key — default `:` |
| `Conflict Strategy` | Enum: `OVERWRITE`, `SKIP`, `FAIL` | What to do if key already exists on target |
| `Write Confirmation` | Boolean | Await pipeline completion before committing session — default `true` |

#### 4.3.1 Batching and Pipelining Implementation

The batch writer accumulates FlowFiles into an in-memory batch, then issues all writes as a single Lettuce async pipeline, awaiting a `RedisFuture` list before committing the NiFi session:

```
onTrigger():
  batch = session.get(MAX_BATCH_SIZE)   // NiFi's multi-get
  if batch.isEmpty(): return

  conn = dragonflyPool.borrowConnection()
  pipeline = conn.async()
  pipeline.setAutoFlushCommands(false)

  futures = []
  for ff in batch:
    payload = parseJSON(session.read(ff))
    cmd = buildWriteCommand(payload, keyPrefix, ttlStrategy)
    futures.add(dispatchCommand(pipeline, cmd))

  pipeline.flushCommands()

  // Await all futures with timeout
  RedisFuture.awaitAll(batchTimeoutMs, TimeUnit.MILLISECONDS, futures)

  // Route based on individual future outcomes
  for (ff, future) in zip(batch, futures):
    if future.isCompletedExceptionally():
      session.transfer(ff, FAILURE)
      log.error("Write failed for key {}", ff.getAttribute("redis.key"), future.cause())
    else:
      session.transfer(ff, SUCCESS)

  session.commit()
  conn.release()
```

**Command dispatch by type:**

```
string  → SET key value [PX ttl]
hash    → HSET key field1 val1 field2 val2 ...
list    → DEL key; RPUSH key val1 val2 ...   (chunked if large)
set     → SADD key val1 val2 ...
zset    → ZADD key score1 member1 score2 member2 ...
stream  → XADD key id field val (per entry); XGROUP CREATE; XSETID
```

**Note on `HSET` batch size:** Dragonfly and Redis both support variadic `HSET key f1 v1 f2 v2 ...` with arbitrary field counts. Use this rather than individual `HSET` calls per field. For very large hashes (\> `Hash Field Batch Size` configured on `RedisTypeDeserializer`), issue multiple variadic `HSET` commands in the same pipeline.

**Note on lists:** Lists cannot be appended atomically to a new key without first deleting it. Issue `DEL key` followed by `RPUSH key val1 val2 ...` in the same pipeline. If `Conflict Strategy` \= `SKIP`, check key existence before the DEL.

**Conflict strategy implementation:**

- `OVERWRITE`: issue DEL before structural types (list, set, zset, stream) — strings, hashes can be directly overwritten.  
- `SKIP`: issue `EXISTS key` as the first command in the pipeline; if result \= 1, skip remaining commands for that key and route to `skipped`.  
- `FAIL`: same as SKIP but route to `failure` instead of `skipped`.

**Relationships:**

| Relationship | Description |
| :---- | :---- |
| `success` | Key written to Dragonfly successfully |
| `skipped` | Key skipped per conflict strategy |
| `failure` | Write failed — route to retry or dead-letter |
| `retry` | Transient failure — route back to this processor's incoming queue |

---

### 4.4 `RedisKeyspaceEventConsumer`

**Purpose:** Subscribes to Redis keyspace notifications on the source, emitting a FlowFile for each key change event. Acts as the live replication layer — runs concurrently with the Scan Phase and continues after it completes until manually stopped.

**Bundle:** `nifi-redis-migration-nar`

**Required properties:**

| Property | Type | Description |
| :---- | :---- | :---- |
| `Redis Connection Pool` | Controller Service | Source `RedisConnectionPoolService` |
| `Keyspace Pattern` | String | Pub/Sub pattern — default `__keyevent@*__:*` |
| `Event Types` | Multi-select | Filter to specific events: `set`, `del`, `expire`, `hset`, `lpush`, etc. |
| `Max Queue Depth` | Integer | Internal buffer before applying back-pressure — default `10000` |
| `Reconnect Backoff (ms)` | Integer | Delay between reconnection attempts — default `1000` |

**Implementation notes:**

- Use Lettuce's `RedisPubSubCommands` with a `RedisPubSubListener` to subscribe asynchronously.  
- Buffer incoming events in a `BlockingQueue<PubSubMessage>`. The `onTrigger()` method drains the queue into FlowFiles and transfers them.  
- If the pub/sub connection drops, log a warning with the timestamp of disconnection and the last event received. Any events delivered during the disconnection window are lost — this is an inherent limitation of Redis pub/sub. Surface this in the NiFi bulletin board.  
- For cluster mode, open a separate pub/sub connection per shard master. NiFi's `Concurrent Tasks` setting on this processor should equal the number of shard masters.  
- **Important:** Keyspace notifications must be enabled on the source before this processor starts. Add a validation check in `onScheduled()` that issues `CONFIG GET notify-keyspace-events` and verifies the response contains `A` and `E`. If not, surface a validation error with instructions for enabling via `CONFIG SET` or AWS parameter group.

**FlowFile attributes emitted:**

| Attribute | Value |
| :---- | :---- |
| `redis.key` | Key name from the event |
| `redis.event.type` | Event type: `set`, `del`, `hset`, `lpush`, etc. |
| `redis.event.db` | Database index from the channel name |
| `redis.event.timestamp` | System timestamp of event receipt (epoch ms) |
| `redis.source.node` | Host:port of the cluster node that emitted the event (cluster mode) |

**Relationships:**

| Relationship | Description |
| :---- | :---- |
| `key_changed` | Key was created or modified — route to `RedisSingleKeyFetch` |
| `key_deleted` | Key was deleted — route to a `DeleteRedisKey` processor on Dragonfly |
| `key_expired` | Key expired on source — optionally propagate deletion to Dragonfly |
| `failure` | Event parsing or queue overflow |

---

### 4.5 `RedisSingleKeyFetch`

**Purpose:** Given a FlowFile with a `redis.key` attribute, fetches the current value of that key from the source Redis and populates FlowFile content identically to `RedisTypeDeserializer`. Used by the live phase.

This processor is a simplified version of `RedisTypeDeserializer` that operates on a single key per FlowFile. Internally reuse the same type-dispatch logic. The implementation detail that differs: call `TYPE key` first to determine the type, then apply the same fetch-and-serialise logic.

---

### 4.6 `MigrationValidationProcessor`

**Purpose:** Post-migration spot-check. Given a list of keys to validate (or a random SCAN sample), fetches values from both source and target, compares them, and emits a validation report FlowFile.

**Required properties:**

| Property | Type | Description |
| :---- | :---- | :---- |
| `Source Connection Pool` | Controller Service | Source `RedisConnectionPoolService` |
| `Target Connection Pool` | Controller Service | Target `DragonflyConnectionPoolService` |
| `Validation Mode` | Enum: `EXISTENCE`, `TYPE`, `FULL_VALUE`, `TTL_DELTA` | Depth of comparison |
| `Sample Size` | Integer | Number of keys to validate per trigger — default `1000` |
| `TTL Delta Tolerance (ms)` | Integer | Acceptable TTL drift between source and target — default `5000` |
| `Report Format` | Enum: `JSON`, `CSV` | Output format for validation report |

**Validation output FlowFile attributes:**

| Attribute | Value |
| :---- | :---- |
| `validation.total_checked` | Count of keys validated |
| `validation.passed` | Count that matched |
| `validation.failed` | Count that did not match |
| `validation.missing_on_target` | Keys present on source but absent on target |
| `validation.type_mismatch` | Keys present on both but with different types |
| `validation.value_mismatch` | Keys with matching types but differing values |

---

## 5\. Complete NiFi Flow Definition

### 5.1 Processor Group Layout

Implement as a top-level `MigrationFlow` Process Group containing two child groups:

```
MigrationFlow (Process Group)
├── ScanPhase (Process Group)
│   ├── RedisScanReader          [Concurrent Tasks: N]
│   ├── RedisTypeDeserializer    [Concurrent Tasks: N]
│   └── RedisBatchWriter         [Concurrent Tasks: W]
│
├── LivePhase (Process Group)
│   ├── RedisKeyspaceEventConsumer  [Concurrent Tasks: S (shards)]
│   ├── RedisSingleKeyFetch         [Concurrent Tasks: N]
│   └── RedisBatchWriter            [shared with ScanPhase or separate]
│
├── FailureHandling (Process Group)
│   ├── [PutFile / PutS3Object]  — dead-letter queue for failed FlowFiles
│   └── [LogAttribute]           — log all failures to bulletin board
│
└── Validation (Process Group)
    └── MigrationValidationProcessor
```

### 5.2 Connection Configuration

All connections between processors must be configured with:

- **Back-pressure object threshold:** `10000` FlowFiles  
- **Back-pressure data threshold:** `1 GB`  
- **FlowFile expiration:** `0 sec` (never expire — do not silently discard)

The failure relationships from `RedisBatchWriter` must connect to a durable sink (file system or S3) for post-migration analysis. Never connect failure relationships to a funnel with no downstream processor.

### 5.3 Parameter Contexts

Define a `MigrationParameters` parameter context with the following parameters, all marked sensitive where appropriate:

```
migration.id                     = <UUID — unique per migration run>
source.connection.string         = redis://... (sensitive)
source.tls.truststore.path       = /path/to/truststore.jks
source.tls.truststore.password   = (sensitive)
source.db.index                  = 0
target.connection.string         = rediss://... (sensitive)
target.tls.truststore.path       = /path/to/truststore.jks
target.tls.truststore.password   = (sensitive)
migration.partition.count        = 8
migration.scan.count             = 200
migration.batch.size             = 500
migration.batch.timeout.ms       = 1000
migration.key.prefix             = (empty or prefix string)
migration.conflict.strategy      = OVERWRITE
migration.ttl.strategy           = PRESERVE
migration.key.pattern            = *
migration.write.tasks            = 4
```

All processor properties that correspond to migration parameters must reference the parameter context using NiFi's `#{parameter.name}` syntax. No credentials or connection strings may appear hardcoded in processor configurations.

---

## 6\. Build and Packaging

### 6.1 Maven Module Structure

```
nifi-redis-migration/
├── pom.xml                                    (parent POM)
├── nifi-redis-migration-processors/
│   ├── pom.xml
│   └── src/main/java/
│       └── io/dragonfly/nifi/redis/
│           ├── processors/
│           │   ├── RedisScanReader.java
│           │   ├── RedisTypeDeserializer.java
│           │   ├── RedisBatchWriter.java
│           │   ├── RedisKeyspaceEventConsumer.java
│           │   ├── RedisSingleKeyFetch.java
│           │   └── MigrationValidationProcessor.java
│           ├── services/
│           │   └── DragonflyConnectionPoolService.java
│           └── util/
│               ├── RedisTypeSerializer.java
│               ├── PartitionAssigner.java
│               └── CommandBuilder.java
└── nifi-redis-migration-nar/
    └── pom.xml                                (NAR packaging)
```

### 6.2 Key Dependencies

```xml
<!-- NiFi API — provided scope (do not bundle) -->
<dependency>
  <groupId>org.apache.nifi</groupId>
  <artifactId>nifi-api</artifactId>
  <version>${nifi.version}</version>
  <scope>provided</scope>
</dependency>

<!-- NiFi Redis services — provided (already in nifi-redis-nar) -->
<dependency>
  <groupId>org.apache.nifi</groupId>
  <artifactId>nifi-redis-service-api</artifactId>
  <version>${nifi.version}</version>
  <scope>provided</scope>
</dependency>

<!-- Lettuce Redis client — bundle in NAR -->
<dependency>
  <groupId>io.lettuce</groupId>
  <artifactId>lettuce-core</artifactId>
  <version>6.3.2.RELEASE</version>
</dependency>

<!-- Jackson for FlowFile content serialisation -->
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
  <version>2.17.1</version>
</dependency>
```

**NAR parent:** Set `nifi-redis-nar` as the NAR parent in `nifi-redis-migration-nar/pom.xml` so the migration NAR can access `RedisConnectionPoolService` without rebundling it.

### 6.3 Processor Registration

Register all processors in:

```
nifi-redis-migration-processors/src/main/resources/META-INF/services/
  org.apache.nifi.processor.Processor
```

Listing each fully-qualified class name, one per line.

---

## 7\. Threading and Concurrency Model

### 7.1 Partition Count Sizing

The recommended formula for partition count N:

```
N = min(available_cpu_cores, ceil(source_key_count / target_keys_per_partition))

where:
  target_keys_per_partition = 500_000  (tunable)
  available_cpu_cores       = NiFi node CPU count × 0.75 (leave headroom)
```

For a 50-million-key source on an 8-core NiFi node: `N = min(6, ceil(50M / 500K)) = min(6, 100) = 6`

### 7.2 Write Concurrency

Writer concurrency `W` is independent of scanner concurrency `N`. Tune `W` based on Dragonfly target capacity:

```
W = floor(dragonfly_core_count / 2)
```

Dragonfly's thread-per-core architecture handles concurrent writers well — each writer's keys will be distributed across Dragonfly shards by hash. There is no benefit to making `W` \> Dragonfly's core count.

### 7.3 Thread Safety Requirements

- `PartitionAssigner` must use `DistributedMapCacheClientService` with CAS (compare-and-swap) semantics to prevent two NiFi tasks from claiming the same partition. Use `replace(key, oldValue, newValue)` not `put`.  
- `RedisBatchWriter`'s internal batch accumulator must be thread-local — do not share batch state across concurrent tasks.  
- The `RedisKeyspaceEventConsumer`'s internal `BlockingQueue` is the only shared state — use `LinkedBlockingQueue` with a bounded capacity matching `Max Queue Depth`.

---

## 8\. Error Handling and Retry

### 8.1 Retry Policy

Implement exponential backoff retry for transient failures in `RedisBatchWriter`:

```
Initial delay:  100ms
Multiplier:     2×
Max delay:      30s
Max attempts:   5
Jitter:         ±20% of current delay
```

Route to `retry` relationship after each failed attempt with `redis.retry.count` attribute incremented. When `redis.retry.count` exceeds `Max attempts`, route to `failure`.

### 8.2 Dead-Letter Queue

All `failure` relationships must connect to a dead-letter processor that:

1. Writes the FlowFile content and all attributes to a structured JSON file in a configurable output directory.  
2. Emits a NiFi bulletin board entry at `ERROR` level with the key name and failure reason.  
3. Increments a NiFi counter (`migration.failures.total`) for monitoring.

### 8.3 Known Incompatible Types

When `RedisScanReader` routes to the `unknown` relationship (module types such as `ReJSON-RL`, `TopK`, `CMSk`):

1. Emit a FlowFile with `redis.incompatible.reason` \= `module_type:<module_name>`.  
2. Route to a `ModuleTypeHandler` processor that attempts `JSON.GET` for ReJSON-RL keys and `TOPK.LIST` for TopK keys, falling back to the dead-letter queue if those commands fail.  
3. Log a bulletin entry listing all incompatible keys found, grouped by module type.

---

## 9\. Security Requirements

### 9.1 Credential Management

- All connection strings, passwords, and trust store paths must be stored in NiFi's **Parameter Context** with sensitive parameters encrypted at rest by NiFi's key provider.  
- No credentials may appear in processor property values, flow.json.gz, or application logs.  
- Use NiFi's `StandardSSLContextService` for TLS configuration rather than passing key material directly to Lettuce.

### 9.2 TLS

- All connections to source Redis and target Dragonfly must use TLS by default. Non-TLS connections require explicit opt-in via a `Require TLS` property set to `false`.  
- Validate server certificates against the configured trust store. Do not set `TRUST_ALL` in any production configuration.  
- Support mutual TLS (mTLS) by accepting optional `Key Store Path` and `Key Store Password` properties on both controller services.

### 9.3 Provenance

NiFi's provenance store is the source of truth for migration audit. Configure the provenance repository with:

- **Repository implementation:** `PersistentProvenanceRepository` (not volatile — survives NiFi restart)  
- **Storage capacity:** Size to hold at least `source_key_count × 2` provenance events (scan \+ write per key)  
- **Rollover size:** `1 GB` per file

The provenance record for each FlowFile will show: which `RedisScanReader` task emitted it, which partition, which cursor position, what the key name and type were, whether `RedisTypeDeserializer` read it successfully, and whether `RedisBatchWriter` wrote it to Dragonfly. This is a complete migration audit trail queryable by key name via NiFi's provenance UI.

---

## 10\. Operational Runbook

### 10.1 Pre-Migration Checklist

- [ ] `notify-keyspace-events` is set to `AE` on the source Redis (for live phase)  
- [ ] Target Dragonfly is reachable from NiFi node(s) with TLS confirmed  
- [ ] Trust stores are deployed to NiFi node(s) at configured paths  
- [ ] `migration.id` parameter is set to a fresh UUID for this migration run  
- [ ] `migration.partition.count` matches `Concurrent Tasks` on `RedisScanReader`  
- [ ] Dead-letter output directory is writable by the NiFi process user  
- [ ] Sufficient disk space for provenance repository (estimate: `key_count × 2KB`)  
- [ ] Dragonfly `--maxmemory` is set to at least `source_used_memory × 1.1`

### 10.2 Migration Start Sequence

1. Start `RedisKeyspaceEventConsumer` first (before scan begins, to capture changes during scan).  
2. Start `RedisTypeDeserializer` and `RedisBatchWriter`.  
3. Start `RedisScanReader` last.  
4. Monitor NiFi's queue depths — all queues should remain bounded. If the queue between `RedisScanReader` and `RedisTypeDeserializer` grows unboundedly, reduce `Concurrent Tasks` on `RedisScanReader` or increase on `RedisTypeDeserializer`.

### 10.3 Cutover Sequence

1. Stop `RedisScanReader` — wait for all in-flight FlowFiles to drain through the pipeline.  
2. Monitor `RedisKeyspaceEventConsumer` queue until empty (live phase has caught up).  
3. Run `MigrationValidationProcessor` — confirm `validation.failed = 0`.  
4. Stop `RedisKeyspaceEventConsumer`.  
5. Redirect application traffic to Dragonfly endpoint.  
6. Stop remaining processors.

### 10.4 Key Metrics to Monitor

| Metric | NiFi Source | Alert Threshold |
| :---- | :---- | :---- |
| Scanner throughput | `RedisScanReader` FlowFiles out/sec | \< 1000/sec sustained |
| Write throughput | `RedisBatchWriter` FlowFiles out/sec | \< 1000/sec sustained |
| Failure rate | `migration.failures.total` counter | \> 0 |
| Queue depth | Scan → Deserialize connection | \> 5000 FlowFiles |
| Queue depth | Deserialize → Writer connection | \> 5000 FlowFiles |
| KSN event lag | `RedisKeyspaceEventConsumer` internal queue | \> 50% of Max Queue Depth |

---

## 11\. References

- Apache NiFi: [https://nifi.apache.org/](https://nifi.apache.org/)  
- Apache NiFi Developer Guide: [https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html](https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html)  
- Apache NiFi REST API: [https://nifi.apache.org/docs/nifi-docs/rest-api/](https://nifi.apache.org/docs/nifi-docs/rest-api/)  
- Lettuce Redis Client: [https://lettuce.io/](https://lettuce.io/)  
- Dragonfly DB: [https://www.dragonflydb.io/](https://www.dragonflydb.io/)  
- Dragonfly Migration Documentation: [https://www.dragonflydb.io/docs/migration](https://www.dragonflydb.io/docs/migration)  
- RedisShake (reference implementation for scan \+ KSN pattern): [https://github.com/tair-opensource/RedisShake](https://github.com/tair-opensource/RedisShake)  
- Redis Keyspace Notifications: [https://redis.io/docs/manual/keyspace-notifications/](https://redis.io/docs/manual/keyspace-notifications/)  
- NiFi Redis NAR source (reference for existing Redis processors): [https://github.com/apache/nifi/tree/main/nifi-extension-bundles/nifi-redis-bundle](https://github.com/apache/nifi/tree/main/nifi-extension-bundles/nifi-redis-bundle)

---

## Appendix A: FlowFile Lifecycle Diagram

```
[RedisScanReader]
    │  attributes: redis.key, redis.type, redis.ttl.ms, redis.partition.index
    │  content: (empty)
    ▼
[RedisTypeDeserializer]
    │  attributes: (unchanged + redis.chunk.index if large)
    │  content: {"key":..., "type":..., "ttl_ms":..., "value":...}
    ▼
[RedisBatchWriter]  ← batches N FlowFiles → single pipelined MULTI-command flush
    │
    ├──[success]────→ provenance record written; FlowFile discarded
    ├──[retry]──────→ back to RedisBatchWriter input queue (with retry counter)
    └──[failure]────→ dead-letter file written; bulletin emitted; counter incremented
```

## Appendix B: Supported Redis → Dragonfly Type Mapping

| Redis Type | OBJECT ENCODING (examples) | Dragonfly write command(s) | Notes |
| :---- | :---- | :---- | :---- |
| string | embstr, raw, int | `SET key value [PX ttl]` | Direct |
| hash | ziplist, listpack, hashtable | `HSET key f1 v1 f2 v2 ...` | Variadic — single command per hash |
| list | listpack, quicklist | `DEL key; RPUSH key v1 v2 ...` | DEL required for new key |
| set | intset, listpack, hashtable | `SADD key v1 v2 ...` |  |
| zset | listpack, skiplist | `ZADD key s1 m1 s2 m2 ...` |  |
| stream | stream | `XADD key id f v; XGROUP CREATE ...` | PEL reconstruction required |
| ReJSON-RL | (module) | `JSON.SET key . <json>` | Via ModuleTypeHandler |
| TopK | (module) | `TOPK.RESERVE; TOPK.ADD` | Default: Approximate reconstruction only, Exact Mode Available  |

