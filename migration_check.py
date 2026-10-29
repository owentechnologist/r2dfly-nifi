#!/usr/bin/env python3
"""
migration_check.py - Assess a Redis or Valkey source for migration to Dragonfly.

Companion to "Migrating to Dragonfly: The Plain-English Guide".
Every finding names the chapter and section of the guide that holds the steps.

The script is READ-ONLY. It runs only INFO, MODULE LIST, ACL LIST, CONFIG GET,
FUNCTION LIST, SCAN, TYPE, DBSIZE, COMMAND INFO, XINFO GROUPS and FT._LIST. It never writes, never
starts replication, and never runs PSYNC.

Usage:
    python3 migration_check.py SOURCE_URI TARGET_URI [options]

    SOURCE_URI / TARGET_URI:
        redis://[user:password@]host:port[/db]     plain TCP
        rediss://[user:password@]host:port[/db]    TLS

Examples:
    python3 migration_check.py redis://10.0.0.5:6379 redis://df.internal:6379
    python3 migration_check.py rediss://default:pw@my.cache.amazonaws.com:6379 \\
        rediss://default:pw@abc.dragonflydb.cloud:6385 --sample 5000 --json

The report suggests a copy tool, RedisShake or r2dfly-nifi (Appendix B), with the
reasons for each and the limits that apply to this source. The r2dfly facts match
r2dfly commit 6afad17 (29 October 2026).

Option --shake-config SHAKE_TOML checks a RedisShake config for filters that
silently drop data (for example allow_command_group used as a type filter).

Exit codes:
    0  at least one pattern is ready with no stop items
    1  every pattern needs work, but none is fully blocked
    2  every pattern is blocked by a stop item
    3  the script could not connect to the source or the target

No third-party packages are needed. Python 3.8 or later.
"""

import argparse
import json
import socket
import ssl
import sys
from urllib.parse import urlparse, unquote

GUIDE = "the Plain-English Guide"

# --------------------------------------------------------------------------
# Guide references: (chapter, section). Section titles match the guide.
# --------------------------------------------------------------------------
R = {
    "p1":        ("Chapter 1", "Pattern 1: Connection swap"),
    "p2":        ("Chapter 1", "Pattern 2: Snapshot load"),
    "p3":        ("Chapter 1", "Pattern 3: Live replication"),
    "pick":      ("Chapter 1", "Pick a pattern"),
    "risk1":     ("Chapter 2", "Pattern 1 risks: no data moves, but functions can still break"),
    "risk2":     ("Chapter 2", "Pattern 2 risks: the file and the gap"),
    "risk3":     ("Chapter 2", "Pattern 3 risks: the stream"),
    "audit":     ("Chapter 3", "Audit your source first"),
    "rdbmap":    ("Chapter 3", "Map your version to its RDB file"),
    "snap5":     ("Chapter 4", "Example: load a Redis 5.0 snapshot"),
    "modules":   ("Chapter 4", "Modules: Dragonfly has native versions of some"),
    "graph":     ("Chapter 4", "RedisGraph is not available"),
    "tracking":  ("Chapter 5", "Client-side caching and RESP3"),
    "acl":       ("Chapter 5", "Move ACL users"),
    "latency":   ("Chapter 5", "Latency monitoring"),
    "functions": ("Chapter 6", "Rewrite Redis Functions as EVAL scripts"),
    "luakeys":   ("Chapter 6", "Declare every key a Lua script uses"),
    "small70":   ("Chapter 6", "Small gaps and their workarounds"),
    "scriptkill":("Chapter 6", "You cannot stop or debug a running script"),
    "rdb12":     ("Chapter 7", "Load an RDB 12 file from a cluster"),
    "noevict":   ("Chapter 7", "CLIENT NO-EVICT and CLIENT NO-TOUCH are not available"),
    "setif":     ("Chapter 8", "Replace conditional SET with a Lua script"),
    "bitop":     ("Chapter 8", "Compose the new BITOP operators"),
    "streams8":  ("Chapter 8", "Stream options from Redis 8"),
    "search":    ("Chapter 8", "Search queries: rewrite the missing options"),
    "vector":    ("Chapter 8", "Vector sets: use a search index"),
    "ts":        ("Chapter 8", "Time series, T-Digest and Auto Suggest are not available"),
    "vk72":      ("Chapter 9", "Valkey 7.2"),
    "vk8":       ("Chapter 9", "Valkey 8.0 and 8.1"),
    "vk9":       ("Chapter 9", "Valkey 9.x"),
    "cluster":   ("Chapter 10", "Cluster mode clients and tools"),
    "aof":       ("Chapter 10", "No append-only file"),
    "admin":     ("Chapter 10", "Introspection and admin gaps"),
    "tools":     ("Chapter 10", "Known live-migration tool failures"),
    "shakefilter": ("Chapter 10", "Filter keys by prefix and by type with RedisShake"),
    "cutover":   ("Chapter 10", "Cutover checklist"),
    "stops":     ("Appendix A: Quick Reference", None),
    "appb_better": ("Appendix B", "Where r2dfly is ahead"),
    "appb_shake":  ("Appendix B", "Where RedisShake is still ahead"),
    "appb_limits": ("Appendix B", "Limits to know before you start"),
    "appb_bench":  ("Appendix B", "Measured against RedisShake"),
    "appb_how":    ("Appendix B", "How r2dfly moves data now"),
    "appb_pick":   ("Appendix B", "Pick a tool"),
    "appb_plan":   ("Appendix B", "Move from RedisShake to r2dfly"),
}

def ref(key):
    ch, sec = R[key]
    return f'{ch}, "{sec}"' if sec else f"{ch} (stop-sign table)"

# Severity order: stop > action > check > info
SEV_RANK = {"stop": 3, "action": 2, "check": 1, "info": 0}
SEV_LABEL = {"stop": "STOP", "action": "ACTION", "check": "CHECK", "info": "INFO"}
ALL = (1, 2, 3)


# --------------------------------------------------------------------------
# Minimal RESP2 client (read-only use)
# --------------------------------------------------------------------------
class RespError(Exception):
    pass


class Conn:
    def __init__(self, uri, timeout=10.0, insecure=False, ca=None):
        u = urlparse(uri)
        if u.scheme not in ("redis", "rediss"):
            raise ValueError(f"unsupported scheme in {uri!r}: use redis:// or rediss://")
        self.host = u.hostname or "localhost"
        self.port = u.port or 6379
        self.tls = u.scheme == "rediss"
        self.user = unquote(u.username) if u.username else None
        self.password = unquote(u.password) if u.password else None
        path = (u.path or "").strip("/")
        self.db = int(path) if path.isdigit() else 0
        raw = socket.create_connection((self.host, self.port), timeout=timeout)
        if self.tls:
            ctx = ssl.create_default_context(cafile=ca) if ca else ssl.create_default_context()
            if insecure:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            raw = ctx.wrap_socket(raw, server_hostname=self.host)
        self.sock = raw
        self.buf = b""
        if self.password is not None:
            if self.user:
                self.cmd("AUTH", self.user, self.password)
            else:
                self.cmd("AUTH", self.password)
        if self.db:
            self.cmd("SELECT", str(self.db))

    def _readline(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed by server")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _readn(self, n):
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed by server")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def _reply(self):
        line = self._readline()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode(errors="replace")
        if t == b"-":
            raise RespError(rest.decode(errors="replace"))
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self._readn(n).decode(errors="replace")
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self._reply() for _ in range(n)]
        raise RespError(f"unexpected reply type {t!r}")

    def cmd(self, *args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(b), b))
        self.sock.sendall(b"".join(out))
        return self._reply()

    def try_cmd(self, *args):
        try:
            return self.cmd(*args), None
        except RespError as e:
            return None, str(e)

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass


def parse_info(text):
    d = {}
    for line in (text or "").splitlines():
        if not line or line.startswith("#") or ":" not in line:
            continue
        k, v = line.split(":", 1)
        d[k.strip()] = v.strip()
    return d


def vtuple(s):
    out = []
    for p in (s or "0").split("."):
        num = "".join(ch for ch in p if ch.isdigit())
        out.append(int(num) if num else 0)
    while len(out) < 3:
        out.append(0)
    return tuple(out[:3])


# --------------------------------------------------------------------------
# Findings
# --------------------------------------------------------------------------
class Findings:
    def __init__(self):
        self.items = []

    def add(self, sev, title, detail, refs, patterns=ALL):
        self.items.append({
            "severity": sev,
            "title": title,
            "detail": detail,
            "guide": list(dict.fromkeys(ref(r) for r in refs)),
            "patterns": list(patterns),
        })

    def for_pattern(self, p):
        return [f for f in self.items if p in f["patterns"]]


# --------------------------------------------------------------------------
# Command rules: base command (lower case) or "cmd|sub" -> rule
# --------------------------------------------------------------------------
def command_rules(src):
    """Return rules keyed by command name. Some rules depend on the source version."""
    r8 = src["family"] == "redis" and src["ver"] >= (8, 0, 0)
    vk81 = src["family"] == "valkey" and src["ver"] >= (8, 1, 0)
    rules = {
        "fcall":        ("action", "Redis Functions (FCALL) are unsupported", "Rewrite each function as a Lua script and call it with EVALSHA.", ["functions"]),
        "fcall_ro":     ("action", "Redis Functions (FCALL_RO) are unsupported", "Rewrite each function as a Lua script and call it with EVALSHA.", ["functions"]),
        "function|load":    ("action", "FUNCTION LOAD is unsupported", "Load scripts with SCRIPT LOAD instead of FUNCTION LOAD.", ["functions"]),
        "function|restore": ("action", "FUNCTION RESTORE is unsupported", "Load scripts with SCRIPT LOAD instead.", ["functions"]),
        "function|dump":    ("action", "FUNCTION DUMP is unsupported", "Keep script sources in the application repository.", ["functions"]),
        "eval":         ("check", "Lua scripts in use (EVAL)", "Each script must pass every key in KEYS.", ["luakeys"]),
        "evalsha":      ("check", "Lua scripts in use (EVALSHA)", "Each script must pass every key in KEYS.", ["luakeys"]),
        "eval_ro":      ("check", "Lua scripts in use (EVAL_RO)", "Each script must pass every key in KEYS.", ["luakeys"]),
        "evalsha_ro":   ("check", "Lua scripts in use (EVALSHA_RO)", "Each script must pass every key in KEYS.", ["luakeys"]),
        "script|kill":  ("stop", "SCRIPT KILL is unsupported", "A running script cannot be stopped on Dragonfly.", ["scriptkill", "stops"]),
        "script|debug": ("stop", "SCRIPT DEBUG is unsupported", "Lua debugging is not available on Dragonfly.", ["scriptkill", "stops"]),
        "script|flush": ("check", "SCRIPT FLUSH in use", "Dragonfly accepts SCRIPT FLUSH with no ASYNC or SYNC option.", ["small70"]),
        "lcs":          ("action", "LCS is unsupported", "Compute the common part in the application.", ["small70"]),
        "copy":         ("check", "COPY in use", "COPY with the DB option is unsupported. Use DUMP, SELECT, RESTORE.", ["small70"]),
        "flushall":     ("check", "FLUSHALL in use", "FLUSHALL ASYNC is unsupported. Use FLUSHALL without ASYNC.", ["small70"]),
        "flushdb":      ("check", "FLUSHDB in use", "FLUSHDB ASYNC is unsupported. Use FLUSHDB without ASYNC.", ["small70"]),
        "client|tracking":   ("action", "Client-side caching (CLIENT TRACKING) in use", "Dragonfly supports CLIENT TRACKING only on RESP3 connections (v1.14 and later), with OPTIN, OPTOUT and NOLOOP. BCAST, PREFIX and REDIRECT are unsupported, so a RESP2 client cannot cache. Use RESP3 with default or OPTIN tracking, or turn caching off. Switch all caching clients at the same time during cutover.", ["tracking", "cutover"]),
        "client|trackinginfo": ("check", "CLIENT TRACKINGINFO is unsupported", "Remove the call.", ["tracking"]),
        "client|getredir":   ("check", "CLIENT GETREDIR is unsupported", "Remove the call.", ["tracking"]),
        "client|no-evict":   ("stop", "CLIENT NO-EVICT is unsupported", "Remove the call. Size memory so eviction does not affect admin clients.", ["noevict", "stops"]),
        "client|no-touch":   ("stop", "CLIENT NO-TOUCH is unsupported", "Remove the call.", ["noevict", "stops"]),
        "client|reply":      ("check", "CLIENT REPLY is unsupported", "Remove the call.", ["noevict"]),
        "client|unblock":    ("check", "CLIENT UNBLOCK is unsupported", "Remove the call.", ["noevict"]),
        "client|kill":       ("check", "CLIENT KILL in use", "MAXAGE and USER filters are unsupported. Kill by ID.", ["small70"]),
        "hello":        ("check", "HELLO in use (possible RESP3 clients)", "Test RESP3 reply shapes, mainly sorted set replies. RESP3 is also what client-side caching needs on Dragonfly.", ["tracking"]),
        "latency":      ("action", "LATENCY commands are unsupported", "Use SLOWLOG, INFO and the Dragonfly Prometheus metrics.", ["latency"]),
        "object|encoding": ("stop", "OBJECT ENCODING is unsupported", "Remove code and tools that read encodings.", ["admin", "stops"]),
        "object|freq":     ("stop", "OBJECT FREQ is unsupported", "No equivalent exists.", ["admin", "stops"]),
        "object|idletime": ("stop", "OBJECT IDLETIME is unsupported", "No equivalent exists.", ["admin", "stops"]),
        "object|refcount": ("check", "OBJECT REFCOUNT is unsupported", "Remove the call.", ["admin"]),
        "swapdb":       ("action", "SWAPDB is unsupported", "Write the new data set to a new database and change SELECT in the app.", ["admin"]),
        "migrate":      ("action", "MIGRATE is unsupported", "Use DUMP and RESTORE, or RedisShake.", ["admin"]),
        "failover":     ("action", "FAILOVER is unsupported", "Promote a replica with REPLICAOF NO ONE.", ["admin"]),
        "memory|purge": ("check", "MEMORY PURGE is unsupported", "Use MEMORY DECOMMIT.", ["admin"]),
        "memory|doctor":("check", "MEMORY DOCTOR is unsupported", "Use INFO memory.", ["admin"]),
        "shutdown":     ("check", "SHUTDOWN in use", "SHUTDOWN ABORT has no equivalent on Dragonfly.", ["admin"]),
        "bgrewriteaof": ("stop", "BGREWRITEAOF is unsupported (no AOF)", "Dragonfly persists with snapshots only.", ["aof", "stops"]),
        "waitaof":      ("stop", "WAITAOF is unsupported (no AOF)", "Use WAIT for replica acknowledgement instead.", ["aof", "stops"]),
        "module|load":   ("action", "MODULE LOAD in use", "MODULE LOAD is unsupported. Use the native Dragonfly versions.", ["modules"]),
        "module|loadex": ("action", "MODULE LOADEX in use", "MODULE LOADEX is unsupported. Use the native Dragonfly versions.", ["modules"]),
        "commandlog":   ("action", "COMMANDLOG is not listed for Dragonfly", "Use SLOWLOG GET.", ["vk8"]),
        "delifeq":      ("action", "DELIFEQ is not listed for Dragonfly", "Use a compare-and-remove Lua script.", ["vk9"]),
        "bf.insert":    ("action", "BF.INSERT is unsupported", "Use BF.RESERVE, then BF.MADD.", ["modules"]),
        "bf.reserve":   ("check", "BF.RESERVE in use", "EXPANSION and NONSCALING are unsupported.", ["modules"]),
        "bf.card":      ("action", "BF.CARD is unsupported", "Track the count in the application.", ["modules"]),
        "cf.scandump":  ("action", "CF.SCANDUMP is unsupported", "Rebuild the Cuckoo filter on the target.", ["modules"]),
        "cf.loadchunk": ("action", "CF.LOADCHUNK is unsupported", "Rebuild the Cuckoo filter on the target.", ["modules"]),
        "ft.search":    ("check", "FT.SEARCH in use", "Many options are missing. Test each production query.", ["search"]),
        "ft.aggregate": ("check", "FT.AGGREGATE in use", "WITHCURSOR, QUANTILE, STDDEV and other options are missing.", ["search"]),
    }
    # Cluster slot admin subcommands
    for sub in ("addslots", "addslotsrange", "delslots", "delslotsrange", "setslot", "meet",
                "forget", "replicate", "reset", "failover", "flushslots", "bumpepoch",
                "set-config-epoch", "countkeysinslot", "getkeysinslot", "saveconfig"):
        rules[f"cluster|{sub}"] = ("stop", f"CLUSTER {sub.upper()} is unsupported",
                                   "Redis cluster admin tools do not work against Dragonfly.", ["cluster", "stops"])
    # Version-dependent option gaps
    if r8 or vk81:
        rules["set"] = ("check", "SET in use on a source that offers IFEQ / IFNE",
                        "Dragonfly lacks SET IFEQ, IFNE, IFDEQ and IFDNE. Search code for these options.",
                        ["setif"] if r8 else ["setif", "vk8"])
    if r8:
        rules["bitop"] = ("check", "BITOP in use on Redis 8", "DIFF, ANDOR and ONE are unsupported.", ["bitop"])
        for c in ("xadd", "xtrim", "xreadgroup", "xsetid"):
            rules[c] = ("check", f"{c.upper()} in use on Redis 8",
                        "ACKED, DELREF, KEEPREF, IDMP and XREADGROUP CLAIM are unsupported.", ["streams8"])
    return rules


# Commands this script sends to the source. Earlier runs leave them in commandstats.
OWN_PROBES = {"info", "module|list", "acl|list", "config|get", "function|list", "scan", "type",
              "dbsize", "auth", "select", "command|info", "ping", "xinfo|groups", "ft._list"}
# Container commands: match only on an exact "cmd|sub" rule, never on the base name.
CONTAINERS = {"client", "object", "cluster", "memory", "script", "function", "module", "acl",
              "config", "command", "latency_dummy"}

PREFIX_RULES = [
    ("ts.",      ("stop", "Time series commands (TS.*) in use", "Time series is not in Dragonfly 2.x.", ["ts", "stops"])),
    ("tdigest.", ("stop", "T-Digest commands in use", "T-Digest is not in Dragonfly 2.x.", ["ts", "stops"])),
    ("ft.sug",   ("stop", "Auto Suggest (FT.SUG*) in use", "Auto Suggest is not in Dragonfly 2.x.", ["ts", "search", "stops"])),
    ("graph.",   ("stop", "RedisGraph commands (GRAPH.*) in use", "Dragonfly has no graph type.", ["graph", "stops"])),
    ("vadd",     ("action", "Vector set commands in use", "Use an FT index with a VECTOR field.", ["vector"])),
    ("vsim",     ("action", "Vector set commands in use", "Use an FT index with a VECTOR field.", ["vector"])),
    ("vrem",     ("action", "Vector set commands in use", "Use an FT index with a VECTOR field.", ["vector"])),
    ("vcard",    ("action", "Vector set commands in use", "Use an FT index with a VECTOR field.", ["vector"])),
    ("vemb",     ("action", "Vector set commands in use", "Use an FT index with a VECTOR field.", ["vector"])),
]

# Module type names as TYPE reports them -> (sev, title, detail, refs, patterns)
TYPE_RULES = {
    "rejson-rl":   ("info",  "JSON keys found (ReJSON-RL)",
                    "Dragonfly has native JSON. RDB JSON loads from v1.16. Older RedisShake builds stop on this type in sync_reader; newer v4 code falls back to RESTORE. Test your build, or use a direct RDB load.",
                    ["modules", "tools", "appb_better"], ALL),
    "topk-type":   ("check", "Top-K keys found", "A Top-K DUMP payload can fail RESTORE. Rebuild with TOPK.RESERVE and TOPK.ADD, or let r2dfly rebuild exact counts.",
                    ["modules", "tools", "appb_better"], (2, 3)),
    "mbbloom--":   ("check", "Bloom filter keys found", "Bloom options EXPANSION and NONSCALING are unsupported. Test the load.", ["modules"], (2, 3)),
    "mbbloomcf":   ("check", "Cuckoo filter keys found", "Test the load. Rebuild the filter if it fails.", ["modules"], (2, 3)),
    "cms-type":    ("check", "Count-Min Sketch keys found", "Test the load.", ["modules"], (2, 3)),
    "tsdb-type":   ("stop",  "Time series keys found", "Time series data cannot move to Dragonfly 2.x.", ["ts", "stops"], ALL),
    "tdis-type":   ("stop",  "T-Digest keys found", "T-Digest data cannot move to Dragonfly 2.x.", ["ts", "stops"], ALL),
    "graphdata":   ("stop",  "Graph keys found", "Graph data cannot move to Dragonfly 2.x.", ["graph", "stops"], ALL),
    "vectorset":   ("action","Vector set keys found", "Move vectors into hashes with an FT VECTOR index.", ["vector"], ALL),
}
NATIVE_TYPES = {"string", "list", "set", "zset", "hash", "stream", "none"}


# --------------------------------------------------------------------------
# Probes
# --------------------------------------------------------------------------
def probe_source(c, sample):
    s = {}
    info = parse_info(c.cmd("INFO", "server"))
    s["server_info"] = info
    name = info.get("server_name", "").lower()
    if name == "valkey" or "valkey_version" in info:
        s["family"] = "valkey"
        s["version_str"] = info.get("valkey_version", info.get("redis_version", "0"))
    else:
        s["family"] = "redis"
        s["version_str"] = info.get("redis_version", "0")
    s["ver"] = vtuple(s["version_str"])
    s["mode"] = info.get("redis_mode", info.get("server_mode", "standalone"))

    stats = parse_info(c.cmd("INFO", "commandstats"))
    cmds = {}
    for k, v in stats.items():
        if not k.startswith("cmdstat_"):
            continue
        name = k[len("cmdstat_"):].lower()
        calls = 0
        for part in v.split(","):
            if part.startswith("calls="):
                calls = int(part.split("=", 1)[1])
        cmds[name] = calls
    s["commands"] = cmds

    s["persistence"] = parse_info(c.cmd("INFO", "persistence"))
    s["memory"] = parse_info(c.cmd("INFO", "memory"))
    s["replication"] = parse_info(c.cmd("INFO", "replication"))
    s["keyspace"] = parse_info(c.cmd("INFO", "keyspace"))

    mods, err = c.try_cmd("MODULE", "LIST")
    names = []
    if isinstance(mods, list):
        for m in mods:
            if isinstance(m, list):
                for i in range(0, len(m) - 1, 2):
                    if str(m[i]).lower() == "name":
                        names.append(str(m[i + 1]).lower())
    s["modules"] = names
    s["module_err"] = err

    acl, err = c.try_cmd("ACL", "LIST")
    s["acl"] = acl if isinstance(acl, list) else []
    s["acl_err"] = err

    s["config"] = {}
    s["config_blocked"] = False
    for key in ("notify-keyspace-events", "maxmemory-policy", "appendonly"):
        val, err = c.try_cmd("CONFIG", "GET", key)
        if err:
            s["config_blocked"] = True
        elif isinstance(val, list) and len(val) >= 2:
            s["config"][key] = val[1]

    funcs, err = c.try_cmd("FUNCTION", "LIST")
    s["function_libs"] = len(funcs) if isinstance(funcs, list) else 0

    # Sample key types with SCAN + TYPE (read-only)
    types = {}
    stream_keys = []
    seen = 0
    cursor = "0"
    while sample > 0:
        res = c.cmd("SCAN", cursor, "COUNT", "500")
        cursor, keys = res[0], res[1]
        for k in keys:
            if seen >= sample:
                break
            t, err = c.try_cmd("TYPE", k)
            if t:
                types[t.lower()] = types.get(t.lower(), 0) + 1
                if t.lower() == "stream" and len(stream_keys) < 50:
                    stream_keys.append(k)
            seen += 1
        if cursor == "0" or seen >= sample:
            break
    s["types"] = types
    s["sampled"] = seen

    # Stream consumer groups with pending entries (read-only XINFO GROUPS)
    pending_streams = 0
    for k in stream_keys:
        groups, err = c.try_cmd("XINFO", "GROUPS", k)
        for g in groups or []:
            if isinstance(g, list):
                d = {str(g[i]): g[i + 1] for i in range(0, len(g) - 1, 2)}
                if int(d.get("pending", 0) or 0) > 0:
                    pending_streams += 1
                    break
    s["pending_streams"] = pending_streams

    # Search indexes (read-only FT._LIST); absent when no search module
    idx, err = c.try_cmd("FT._LIST")
    s["search_indexes"] = idx if isinstance(idx, list) else []
    s["dbsize"] = c.cmd("DBSIZE")
    return s


def probe_target(c, src_cmds):
    t = {}
    info = parse_info(c.cmd("INFO", "server"))
    t["server_info"] = info
    t["is_dragonfly"] = "dragonfly_version" in info
    t["version_str"] = info.get("dragonfly_version", info.get("redis_version", "0")).lstrip("df-v")
    t["ver"] = vtuple(t["version_str"])
    mem = parse_info(c.cmd("INFO", "memory"))
    t["maxmemory"] = int(mem.get("maxmemory", "0") or 0)
    t["dbsize"] = c.cmd("DBSIZE")
    # Which source base commands does the target know? COMMAND INFO is read-only.
    bases = sorted({n.split("|")[0] for n in src_cmds})
    missing = []
    known_err = None
    for i in range(0, len(bases), 50):
        chunk = bases[i:i + 50]
        res, err = c.try_cmd("COMMAND", "INFO", *chunk)
        if err:
            known_err = err
            break
        for name, entry in zip(chunk, res or []):
            if entry is None:
                missing.append(name)
    t["missing_commands"] = missing
    t["command_info_err"] = known_err
    return t


# --------------------------------------------------------------------------
# Assessment
# --------------------------------------------------------------------------
def rdb_for(src):
    fam, v = src["family"], src["ver"]
    if fam == "redis":
        if v < (3, 2, 0): return "6"
        if v < (4, 0, 0): return "7"
        if v < (5, 0, 0): return "8"
        if v < (7, 0, 0): return "9"
        if v < (7, 2, 0): return "10"
        if v < (7, 4, 0): return "11"
        if v < (8, 0, 0): return "12"
        return "see header"
    if v < (8, 0, 0): return "11"
    return "see header"


def assess(src, tgt, src_uri, f):
    fam, v = src["family"], src["ver"]
    cmds = src["commands"]
    host = (urlparse(src_uri).hostname or "").lower()

    # ---- Target sanity
    if not tgt["is_dragonfly"]:
        f.add("action", "Target does not report a Dragonfly version",
              "INFO server has no dragonfly_version field. Check the target URI.", ["audit"])
    elif tgt["ver"] < (2, 0, 0):
        f.add("check", f"Target runs Dragonfly {tgt['version_str']}",
              "The guide describes Dragonfly 2.x. Upgrade, or check the compatibility table for your release.", ["audit"])
    if tgt["dbsize"]:
        f.add("check", f"Target already holds {tgt['dbsize']} keys",
              "A snapshot load or live copy can overwrite or mix with these keys.", ["risk2", "risk3"], (2, 3))

    # ---- Version and RDB (Pattern 2)
    rdb = rdb_for(src)
    if rdb == "see header":
        if fam == "valkey" and v >= (9, 0, 0):
            if tgt["ver"] >= (2, 0, 0):
                f.add("check", f"Valkey {src['version_str']} snapshot",
                      "Dragonfly v2.0.0 adds Valkey 9 RDB loading. Run a test load first.", ["vk9", "rdbmap"], (2,))
            else:
                f.add("action", f"Valkey {src['version_str']} snapshot needs Dragonfly v2.0.0 or later",
                      "Upgrade the target before a snapshot load.", ["vk9", "rdbmap"], (2,))
        else:
            f.add("check", f"{fam.title()} {src['version_str']} RDB version is not mapped in the guide",
                  "Read the first nine bytes of dump.rdb and run a test load.",
                  ["rdbmap", "vk8" if fam == "valkey" else "rdbmap"], (2,))
    else:
        f.add("info", f"Source writes RDB {rdb}",
              "Dragonfly 2.x loads this RDB version.",
              ["rdbmap", "snap5" if rdb in ("6", "7", "8", "9") else "rdbmap"], (2,))
        if rdb == "12" and src["mode"] == "cluster":
            f.add("info", "RDB 12 from a cluster",
                  "Dragonfly drops the slot info and logs a warning. The keys load.", ["rdb12"], (2,))
    if fam == "valkey" and v < (8, 0, 0):
        f.add("info", "Valkey 7.2 source", "Valkey 7.2 matches Redis 7.2.", ["vk72"])

    # ---- Commands in use
    rules = command_rules(src)
    hit_titles = set()
    for name, calls in sorted(cmds.items()):
        if name in OWN_PROBES:
            continue
        base = name.split("|")[0]
        rule = rules.get(name)
        if rule is None and base not in CONTAINERS:
            rule = rules.get(base)
        if rule is None:
            for pre, r in PREFIX_RULES:
                if base.startswith(pre):
                    rule = r
                    break
        if rule and rule[1] not in hit_titles:
            hit_titles.add(rule[1])
            sev, title, detail, refs = rule
            f.add(sev, title, f"{detail} ({calls} call{'s' if calls != 1 else ''} since stats reset.)", refs, ALL)

    known_bases = {k.split("|")[0] for k in rules} | CONTAINERS
    unmatched = [m for m in tgt["missing_commands"]
                 if m not in known_bases and not any(m.startswith(p) for p, _ in PREFIX_RULES)]
    if unmatched:
        f.add("action", "Commands in use that the target does not list",
              "COMMAND INFO on the target returned nothing for: " + ", ".join(unmatched[:25]) +
              (" ..." if len(unmatched) > 25 else "") + ". Check each one in the compatibility table.",
              ["risk1", "audit"], ALL)
    if tgt["command_info_err"]:
        f.add("check", "Could not check commands on the target",
              f"COMMAND INFO failed: {tgt['command_info_err']}. Check commands by hand.", ["risk1"], ALL)
    if src["function_libs"]:
        f.add("action", f"{src['function_libs']} Redis Function libraries loaded on the source",
              "Rewrite each function as an EVAL script before cutover.", ["functions"], ALL)

    # ---- Modules and key types
    for m in src["modules"]:
        if m in ("graph",):
            f.add("stop", "RedisGraph module loaded", "Dragonfly has no graph type.", ["graph", "stops"])
        elif m in ("timeseries",):
            f.add("stop", "Time series module loaded", "Time series is not in Dragonfly 2.x.", ["ts", "stops"])
        elif m in ("search", "ft"):
            f.add("check", "Search module loaded", "Rebuild indexes on Dragonfly. Rewrite missing options.", ["search"])
        elif m in ("rejson", "redisjson"):
            f.add("info", "JSON module loaded", "Dragonfly has native JSON commands.", ["modules"])
        elif m in ("bf", "redisbloom"):
            f.add("check", "Bloom module loaded", "Dragonfly has partial native Bloom and Cuckoo support.", ["modules"])
        elif m not in ("vectorset",):
            f.add("action", f"Module '{m}' loaded",
                  "Dragonfly does not load external modules. Move the logic to the application.", ["modules"])
    for tname, n in src["types"].items():
        if tname in NATIVE_TYPES:
            continue
        rule = TYPE_RULES.get(tname)
        if rule:
            sev, title, detail, refs, pats = rule
            f.add(sev, f"{title}: {n} in sample", detail, refs, pats)
        else:
            f.add("action", f"Unknown key type '{tname}': {n} in sample",
                  "Check the type in the compatibility table. Test a load.", ["modules", "risk2"], ALL)

    # ---- ACL
    patterned = [u for u in src["acl"] if isinstance(u, str) and
                 (" ~" in u and "~*" not in u or " &" in u and "&*" not in u or "%r~" in u.lower()
                  or "%w~" in u.lower() or "(" in u)]
    if patterned:
        f.add("action", f"{len(patterned)} ACL users with key, channel or selector rules",
              "Test ACL LOAD on Dragonfly. Apply patterns with ACL SETUSER and check with ACL DRYRUN.",
              ["acl"] + (["vk8"] if fam == "valkey" else []), ALL)
    elif src["acl_err"]:
        f.add("check", "Could not read ACL LIST", f"{src['acl_err']}. Export users by hand.", ["acl"], ALL)

    # ---- Client-side caching: protocol and target version
    tracking_calls = sum(v for k, v in cmds.items() if k == "client|tracking")
    if tracking_calls:
        if cmds.get("hello"):
            for it in f.items:
                if it["title"] == "Client-side caching (CLIENT TRACKING) in use":
                    it["severity"] = "check"
                    it["detail"] += (" HELLO calls suggest RESP3 clients, which work on Dragonfly unless they "
                                     "use BCAST or PREFIX. Check the client's caching mode.")
        else:
            f.add("action", "Caching clients probably use RESP2",
                  "The source shows CLIENT TRACKING calls but no HELLO calls. A RESP2 client receives invalidations "
                  "through REDIRECT, which Dragonfly does not support. Move these clients to RESP3, or turn "
                  "client-side caching off.", ["tracking"], ALL)
        if tgt["is_dragonfly"] and tgt["ver"] < (1, 14, 0):
            f.add("action", f"Target Dragonfly {tgt['version_str']} has no CLIENT TRACKING",
                  "CLIENT TRACKING is new in Dragonfly v1.14. Upgrade the target, or turn client-side caching off.",
                  ["tracking"], ALL)

    # ---- Cluster mode
    if src["mode"] == "cluster":
        f.add("check", "Source runs in cluster mode",
              "Cluster clients need Dragonfly emulated cluster mode. Freeze the topology during a live copy.",
              ["cluster"], ALL)
        f.add("check", "Live copy from a cluster", "A resize or failover stops a RedisShake run.", ["tools", "risk3"], (3,))

    # ---- Persistence
    if src["persistence"].get("aof_enabled") == "1" or src["config"].get("appendonly") == "yes":
        f.add("check", "Source uses AOF persistence",
              "Dragonfly persists with snapshots only. Accept the snapshot interval as the maximum data loss.",
              ["aof"], ALL)

    # ---- Pattern 3 tooling: PSYNC and keyspace notifications
    managed = any(h in host for h in ("cache.amazonaws.com", "redis.cache.windows.net",
                                       "memorystore", "redislabs.com", "redis-cloud.com", "upstash.io"))
    src["managed"] = managed
    if managed:
        f.add("check", "Source looks like a managed service",
              "Managed services often block PSYNC and CONFIG SET. Plan for keyspace notifications set in the service's "
              "parameter group: RedisShake scan_reader with ksn = true, or r2dfly's default keyspace live source.",
              ["p3", "tools"], (3,))
    if src["config_blocked"]:
        f.add("check", "CONFIG GET is blocked on the source",
              "You cannot read or set notify-keyspace-events here. On ElastiCache, use a parameter group.",
              ["tools"], (3,))
    else:
        nke = src["config"].get("notify-keyspace-events", "")
        has_all = "A" in nke or all(ch in nke for ch in "g$lshztxe")
        if not (has_all and "E" in nke):
            f.add("action", f"notify-keyspace-events is '{nke or 'empty'}'",
                  "RedisShake scan_reader with ksn = true needs 'AE'. Set it before a live copy.",
                  ["tools", "p3"], (3,))
    f.add("info", "Keyspace notifications do not report FLUSHALL or FLUSHDB",
          "Block those commands on the source during a live copy.", ["tools"], (3,))

    # ---- Capacity
    used = int(src["memory"].get("used_memory", "0") or 0)
    if tgt["maxmemory"] and used > tgt["maxmemory"]:
        f.add("stop", "Target maxmemory is smaller than the source data",
              f"Source used_memory {used:,} bytes > target maxmemory {tgt['maxmemory']:,} bytes. "
              "Resize the target (not covered in the guide).", ["audit"], (2, 3))

    # ---- Cache vs durable hint
    policy = src["config"].get("maxmemory-policy", "")
    src["looks_like_cache"] = policy.startswith("allkeys") or policy.startswith("volatile")
    return f



# --------------------------------------------------------------------------
# RedisShake config (shake.toml) analysis
# --------------------------------------------------------------------------
def load_toml(path):
    """tomllib on Python 3.11+, tomli if installed, else a small parser for shake.toml."""
    try:
        import tomllib
        with open(path, "rb") as fh:
            return tomllib.load(fh)
    except ImportError:
        pass
    try:
        import tomli
        with open(path, "rb") as fh:
            return tomli.load(fh)
    except ImportError:
        pass
    import ast, re
    data = {}
    cur = data
    text = open(path, encoding="utf-8").read()
    i, lines = 0, text.splitlines()
    while i < len(lines):
        line = lines[i].strip()
        i += 1
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^\[([^\]]+)\]$", line)
        if m:
            cur = data.setdefault(m.group(1).strip(), {})
            continue
        if "=" not in line:
            continue
        k, v = [x.strip() for x in line.split("=", 1)]
        if v.startswith('"""'):
            body = v[3:]
            while '"""' not in body and i < len(lines):
                body += "\n" + lines[i]
                i += 1
            cur[k] = body.split('"""')[0]
            continue
        v = re.sub(r"\s+#.*$", "", v)
        while v.startswith("[") and v.count("[") > v.count("]") and i < len(lines):
            v += lines[i].strip()
            i += 1
        if v in ("true", "false"):
            cur[k] = v == "true"
        else:
            try:
                cur[k] = ast.literal_eval(v)
            except Exception:
                cur[k] = v.strip('"')
    return data


TYPE_GROUPS = {"STRING", "LIST", "SET", "SORTED_SET", "HASH", "STREAM", "GEO", "HYPERLOGLOG", "BITMAP"}
MULTI_KEY_CMDS = {"mset", "msetnx", "mget", "del", "unlink", "rename", "renamenx", "smove", "sunionstore",
                  "sinterstore", "sdiffstore", "zunionstore", "zinterstore", "zdiffstore", "lmove",
                  "rpoplpush", "blmove", "brpoplpush", "copy", "bitop", "pfmerge"}


def analyze_shake(cfg, src, f, path):
    P3 = (3,)
    readers = [k for k in ("sync_reader", "scan_reader", "rdb_reader", "aof_reader") if k in cfg]
    if not readers:
        f.add("action", f"No reader section in {path}",
              "Add a sync_reader, scan_reader or rdb_reader section.", ["p3"], P3)
        return
    reader = readers[0]
    rcfg = cfg.get(reader, {}) or {}
    flt = cfg.get("filter", {}) or {}
    adv = cfg.get("advanced", {}) or {}
    snapshot = reader in ("scan_reader", "rdb_reader") or (reader == "sync_reader" and rcfg.get("sync_rdb", True) is not False)
    up = lambda xs: [str(x).upper() for x in (xs or [])]
    allow_grp, block_grp = up(flt.get("allow_command_group")), up(flt.get("block_command_group"))
    allow_cmd, block_cmd = up(flt.get("allow_command")), up(flt.get("block_command"))
    func = flt.get("function", "") or ""
    prefixes = (flt.get("allow_key_prefix") or []) + (flt.get("allow_key_regex") or []) + \
               (flt.get("allow_keys") or []) + (flt.get("allow_key_suffix") or [])
    cmds = src["commands"]

    f.add("info", f"RedisShake config: {reader}" + (" with a snapshot phase" if snapshot else " (live phase only)"),
          "In the snapshot phase, RedisShake sends each key as one RESTORE command (group GENERIC). "
          "Only keys above target_redis_proto_max_bulk_len arrive as typed commands.", ["shakefilter"], P3)

    # Command-group filters used as type filters
    if allow_grp and snapshot:
        if "GENERIC" not in allow_grp:
            f.add("stop", f"allow_command_group = {allow_grp} drops every snapshot key",
                  "RESTORE is in the GENERIC group, so the snapshot phase copies nothing (scan) or only live writes "
                  "(sync). There is no error. Use skip_unknown_type (scan_reader) or a Lua function that reads the type.",
                  ["shakefilter", "stops"], P3)
        else:
            f.add("check", "allow_command_group includes GENERIC",
                  "Every snapshot key passes, so the group list does not filter snapshot keys by type.",
                  ["shakefilter"], P3)
    if block_grp and snapshot:
        if "GENERIC" in block_grp:
            f.add("stop", "block_command_group blocks GENERIC",
                  "This drops every snapshot RESTORE, and live DEL and EXPIRE commands.", ["shakefilter", "stops"], P3)
        typed = sorted(set(block_grp) & TYPE_GROUPS)
        if typed:
            f.add("action", f"block_command_group = {typed} does not filter snapshot keys",
                  "Snapshot keys of these types still copy as RESTORE. Only live writes are blocked, so the target "
                  "holds stale copies. Use skip_unknown_type or a Lua function.", ["shakefilter"], P3)
    if allow_cmd and snapshot and "RESTORE" not in allow_cmd:
        f.add("stop", f"allow_command = {allow_cmd} has no RESTORE",
              "Every snapshot key is a RESTORE command, so the snapshot phase copies nothing.", ["shakefilter", "stops"], P3)
    if "RESTORE" in block_cmd and snapshot:
        f.add("stop", "block_command contains RESTORE",
              "Every snapshot key is a RESTORE command, so the snapshot phase copies nothing.", ["shakefilter", "stops"], P3)

    # skip_unknown_type
    skip = [str(x).lower() for x in (rcfg.get("skip_unknown_type") or [])]
    for other in readers:
        if other != "scan_reader" and (cfg.get(other) or {}).get("skip_unknown_type"):
            f.add("action", f"skip_unknown_type is set under [{other}]",
                  "Only scan_reader reads this option. RedisShake ignores it here, so every type copies.",
                  ["shakefilter"], P3)
    if skip and reader == "scan_reader":
        sampled = sorted(t for t in src["types"] if t not in skip and t != "none")
        f.add("info", f"skip_unknown_type skips {skip}",
              "This is a block list. In the source sample, these types still copy: "
              + (", ".join(sampled) if sampled else "none") + ".", ["shakefilter"], P3)
        if not rcfg.get("ksn"):
            f.add("check", "scan_reader without ksn",
                  "Writes after each key is scanned are missed. Set ksn = true if writes continue.",
                  ["shakefilter", "tools"], P3)

    # Lua function
    if func.strip():
        if "shake.call" not in func:
            f.add("stop", "The filter function never calls shake.call",
                  "RedisShake drops every command that reaches the function.", ["shakefilter", "stops"], P3)
        elif "GROUP" in func and "RESTORE" not in func and snapshot:
            f.add("action", "The filter function tests GROUP but not RESTORE",
                  "Snapshot keys arrive as RESTORE with GROUP = GENERIC, so a type test on GROUP drops them. "
                  "Read the RDB type byte of the RESTORE payload.", ["shakefilter"], P3)
        elif "RESTORE" in func:
            f.add("check", "The filter function handles RESTORE",
                  "Check the RDB type codes for your source version (codes 22-25 are Redis 7.4 hash encodings).",
                  ["shakefilter"], P3)
        if "RENAME" not in func.upper() and cmds.get("rename", 0) + cmds.get("renamenx", 0) > 0:
            f.add("check", "Source uses RENAME, and the function has no RENAME rule",
                  "A renamed key can go stale on the target. Block renames during the copy, or add a rule.",
                  ["shakefilter"], P3)

    # Key filters and multi-key commands
    if prefixes or flt.get("block_key_prefix") or flt.get("block_key_regex"):
        used = sorted(c for c in cmds if c in MULTI_KEY_CMDS and cmds[c] > 0)
        if used:
            f.add("check", "Key filters with multi-key commands on the source",
                  "RedisShake drops a multi-key command unless every key passes the filter. Used here: "
                  + ", ".join(used) + ".", ["shakefilter", "tools"], P3)

    uses_ksn = reader == "scan_reader" and bool(rcfg.get("ksn"))
    if not uses_ksn:
        for it in f.items:
            if it["title"].startswith("notify-keyspace-events is") or it["title"].startswith("Keyspace notifications do not report"):
                it["severity"] = "info"
                it["detail"] += " Not needed for this RedisShake config, which does not use KSN."

    if reader == "sync_reader" and src["config_blocked"]:
        f.add("check", "sync_reader against a source that blocks CONFIG",
              "Managed services that block CONFIG often block PSYNC too. Plan for scan_reader.", ["tools", "p3"], P3)



# --------------------------------------------------------------------------
# Tool suggestion: RedisShake or r2dfly-nifi (Appendix B)
# --------------------------------------------------------------------------
FIELD_TTL_CMDS = {"hexpire", "hpexpire", "hexpireat", "hpexpireat", "hpersist", "hsetex", "hgetex"}
# Facts below match r2dfly at commit 6afad17 (29 October 2026) and its verified-behavior page.
R2DFLY_VERSION = "6afad17 (29 October 2026)"


def suggest_tool(src, tgt, f, shake_cfg=None):
    """Suggest RedisShake or r2dfly-nifi for the copy. Never changes a pattern's status."""
    types, cmds = src["types"], src["commands"]
    fam, v = src["family"], src["ver"]
    durable = not src.get("looks_like_cache")
    df_source = "dragonfly_version" in src.get("server_info", {})
    for_r2, for_shake, limits, checks = [], [], [], []
    if df_source:
        for_r2.append("Dragonfly source: r2dfly has a Dragonfly-to-Dragonfly path that also copies Bloom, Cuckoo "
                      "and Count-Min Sketch keys. Dragonfly has no PSYNC and publishes only expiry notifications, "
                      "so its live phase uses reconcile passes")

    # -- Payloads that Dragonfly rejects through RESTORE (RedisShake writes every snapshot key with RESTORE)
    new_payloads = (fam == "redis" and v >= (8, 0, 0)) or (fam == "valkey" and v >= (9, 0, 0))
    if new_payloads:
        for_r2.append(f"{fam.title()} {src['version_str']} source: Dragonfly 2.0.2 rejected every DUMP payload "
                      "from Redis 8 and Valkey 9 in r2dfly's tests. RedisShake writes snapshot keys with RESTORE; "
                      "r2dfly falls back to typed commands")
        f.add("check", f"RESTORE payloads from {fam.title()} {src['version_str']} may be rejected by Dragonfly",
              "Dragonfly 2.0.2 rejected DUMP payloads from Redis 8.10 and Valkey 9.1 ('payload version or checksum "
              "are wrong'). A RESTORE-based copy (RedisShake) fails these keys. Test one key, or use a tool with a "
              "typed fallback.", ["appb_limits", "tools"], (3,))
    used_ttl = sorted(c for c in cmds if c in FIELD_TTL_CMDS and cmds[c] > 0)
    if used_ttl and fam == "redis" and v >= (7, 4, 0):
        for_r2.append("hash field TTLs in use (" + ", ".join(used_ttl) + "): Dragonfly rejects RESTORE of a Redis 7.4 "
                      "hash with field TTLs; r2dfly copies them with HPTTL and HEXPIRE at one-second precision")
        f.add("check", "Hashes with field TTLs cannot be copied with RESTORE",
              "Dragonfly 2.0.2 rejected Redis 7.4 hashes with field TTLs ('Bad data format'). Copy them with typed "
              "commands (HSET, then HEXPIRE).", ["appb_limits"], (3,))

    # -- Module keys and indexes
    if types.get("rejson-rl") or "rejson" in src["modules"] or "redisjson" in src["modules"]:
        for_r2.append("JSON keys: r2dfly rebuilds them with JSON.GET and JSON.SET, also during the live phase")
    if types.get("topk-type"):
        for_r2.append("Top-K keys: r2dfly rebuilds them. In the project's test, RedisShake stopped at the first "
                      "rejected Top-K RESTORE")
        limits.append("Top-K counts can differ from the source when items collide, because Dragonfly counts "
                      "differently from RedisBloom")
    if src.get("search_indexes"):
        for_r2.append(f"{len(src['search_indexes'])} search index(es): r2dfly exports them and creates them again")

    # -- Filters
    shake_type_filter = [i for i in f.items if i["title"].startswith((
        "allow_command_group", "block_command_group", "skip_unknown_type",
        "The filter function handles RESTORE", "The filter function tests GROUP"))]
    if shake_type_filter:
        for_r2.append("the RedisShake config filters by type: r2dfly filters by real key type in the copy, the live "
                      "phase and the reconciler. ./import-shake.sh translates the config")
    if shake_cfg:
        flt = shake_cfg.get("filter", {}) or {}
        fine = [k for k in ("allow_key_suffix", "block_key_suffix", "allow_key_regex", "block_key_regex",
                            "allow_slot", "block_slot") if flt.get(k)]
        func = (flt.get("function") or "").strip()
        if func and not shake_type_filter:
            fine.append("Lua function")
        if fine:
            for_shake.append("the RedisShake config selects keys by " + ", ".join(fine) +
                             ". r2dfly filters by prefix and type only. Its Groovy hook can drop other keys, "
                             "but the reconciler keeps reporting them, so the cutover check cannot pass")

    # -- Continuous migration, restart and topology
    if durable:
        for_r2.append("durable data with live writes: r2dfly keeps the target current until cutover, repairs gaps "
                      "with its reconciler, and cutover.sh says when it is safe to switch")
    managed_src = src.get("managed") or src["config_blocked"]
    if src["mode"] == "cluster" and not managed_src:
        for_r2.append("cluster source: with --live-source replication, r2dfly follows failovers, resharding and new "
                      "shards. RedisShake stops on a topology change")
    elif src["mode"] == "cluster":
        for_r2.append("cluster source: r2dfly's copy reads a slot that was migrating once it settles. Following "
                      "failovers in the live phase needs --live-source replication, which this managed source "
                      "probably blocks")
    if (src.get("dbsize") or 0) >= 5_000_000:
        for_r2.append(f"{src['dbsize']:,} keys: r2dfly resumes from scan checkpoints and a partial resync after a "
                      "restart. RedisShake starts over")

    # -- Speed and operation
    bloomish = any(types.get(t) for t in ("mbbloom--", "mbbloomcf", "cms-type")) and not df_source
    if not durable and not for_r2 and not bloomish:
        for_shake.append("cache-like source with no keys that need special handling: RedisShake sync_reader copied small keys about 1.7 to "
                         "2.1 times faster in the project's benchmark, and it is one binary")
    elif (src.get("dbsize") or 0) >= 1_000_000:
        limits.append("bulk copy of many small keys is slower than RedisShake sync_reader (about 38,000 against "
                      "64,000 strings per second in the project's benchmark)")

    # -- Limits that need action
    prob = [t for t in ("mbbloom--", "mbbloomcf", "cms-type") if types.get(t)]
    if prob and not df_source:
        for_r2.append("Bloom-family keys present: in the project's test RedisShake stopped at the first rejected "
                      "Bloom RESTORE; r2dfly dead-letters each one and copies every other key")
        checks.append("Bloom, Cuckoo or Count-Min Sketch keys (" + ", ".join(prob) + ") cannot reach Dragonfly with "
                      "any tool. r2dfly dead-letters each one with the reason; RedisShake stops at the first one")
    if tgt.get("dbsize"):
        n = tgt["dbsize"]
        checks.append(f"the target already holds {n:,} key{'s' if n != 1 else ''}. r2dfly's reconciler deletes target keys the "
                      "source does not have; use --reconcile-spare-target-keys to keep them")
    managed = src.get("managed") or src["config_blocked"]
    if managed and not df_source:
        checks.append("managed source: r2dfly does not support the renamed PSYNC command (aws_psync). Use the default "
                      "keyspace live source, with notify-keyspace-events set in the parameter group")
    if not df_source:
        limits.append("with the keyspace live source, a dropped connection loses changes until the reconciler "
                      "repairs them; a FULL reconcile pass finds every gap")
    limits.append("a headless (no NiFi server) copy does not resume after a crash")

    src["tool_reasons"], src["tool_shake"] = for_r2, for_shake
    src["tool_limits"], src["tool_hard_limits"] = limits, checks
    if for_r2 and (not for_shake or len(for_r2) > len(for_shake)):
        live = ("--live-source reconcile" if df_source else
                "--live-source replication" if (src["mode"] == "cluster" and not managed) else
                "the keyspace live source")
        src["tool_suggestion"] = (f"r2dfly: continuous-migration.sh with {live}, then cutover.sh" if durable
                                  else "r2dfly: simple-migration.sh (one-time copy)")
    elif for_shake and not for_r2:
        src["tool_suggestion"] = "RedisShake sync_reader (or a connection swap for a pure cache)"
    elif for_r2 and for_shake:
        src["tool_suggestion"] = "Either tool: compare the reasons listed for each"
    else:
        src["tool_suggestion"] = "RedisShake (or a snapshot load)"
    src["tool_refs"] = [ref("appb_better"), ref("appb_shake"), ref("appb_limits")] + \
                       ([ref("appb_plan")] if for_r2 else [])


def pattern_status(items):
    worst = max([SEV_RANK[i["severity"]] for i in items], default=0)
    if worst >= 3:
        return "BLOCKED"
    if worst == 2:
        return "READY AFTER WORK"
    if worst == 1:
        return "READY AFTER CHECKS"
    return "READY"


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------
PATTERN_NAMES = {1: "Pattern 1: Connection swap", 2: "Pattern 2: Snapshot load", 3: "Pattern 3: Live replication"}
PATTERN_REF = {1: "p1", 2: "p2", 3: "p3"}


def print_report(src, tgt, src_uri, tgt_uri, f):
    w = 78
    print("=" * w)
    print("Dragonfly migration assessment".center(w))
    print(f"Steps refer to {GUIDE}".center(w))
    print("=" * w)
    print(f"Source : {redact(src_uri)}")
    print(f"         {src['family'].title()} {src['version_str']}, mode {src['mode']}, "
          f"{src['dbsize']:,} keys, {len(src['commands'])} distinct commands used")
    print(f"         key types in sample of {src['sampled']}: " +
          (", ".join(f"{k}={v}" for k, v in sorted(src['types'].items())) or "none"))
    if src.get("search_indexes") or src.get("pending_streams"):
        print(f"         search indexes: {len(src.get('search_indexes', []))}, "
              f"streams with pending entries in sample: {src.get('pending_streams', 0)}")
    print(f"Target : {redact(tgt_uri)}")
    print(f"         {'Dragonfly' if tgt['is_dragonfly'] else 'NOT Dragonfly'} {tgt['version_str']}, "
          f"{tgt['dbsize']:,} keys")
    print()

    statuses = {}
    for p in (1, 2, 3):
        items = f.for_pattern(p)
        statuses[p] = pattern_status(items)

    print("-" * w)
    print("Summary by pattern")
    print("-" * w)
    for p in (1, 2, 3):
        print(f"  {PATTERN_NAMES[p]:<32} {statuses[p]:<20} steps: {ref(PATTERN_REF[p])}")
    print()
    rec = recommend(src, statuses)
    print(f"  Suggested pattern: {rec}")
    print(f"  How to choose: {ref('pick')}")
    print(f"  Suggested copy tool: {src.get('tool_suggestion', 'RedisShake')}")
    print(f"  Tool comparison: {ref('appb_pick')}")
    print()
    print("-" * w)
    print(f"Copy tool: {src.get('tool_suggestion', 'RedisShake')}")
    print(f"(r2dfly facts as of commit {R2DFLY_VERSION}; this section does not change any pattern status)")
    print("-" * w)
    def bullets(title, items, marker_for=None):
        if not items:
            return
        print(f"  {title}")
        for it in items:
            mark = "[CHECK]" if marker_for and it in marker_for else "-      "
            for i_, line in enumerate(wrap(it, w - 16)):
                print((f"    {mark} " if i_ == 0 else "            ") + line)
    bullets("Why r2dfly helps here:", src.get("tool_reasons", []))
    bullets("Why RedisShake may fit better:", src.get("tool_shake", []))
    bullets("Act on these before cutover, whichever tool you use:", src.get("tool_hard_limits", []),
            src.get("tool_hard_limits", []))
    if src.get("tool_reasons"):
        bullets("r2dfly limits that apply to this source:", src.get("tool_limits", []))
    for g in src.get("tool_refs", []):
        print(f"    -> {g}")
    print()

    def show(items):
        if not items:
            print("  No findings.")
        for i in sorted(items, key=lambda i: -SEV_RANK[i["severity"]]):
            print(f"  [{SEV_LABEL[i['severity']]:<6}] {i['title']}")
            for line in wrap(i["detail"], w - 11):
                print(f"           {line}")
            for g in i["guide"]:
                print(f"           -> {g}")
        print()

    shared = [i for i in f.items if sorted(i["patterns"]) == [1, 2, 3]]
    print("-" * w)
    print("Findings for every pattern (the application sends these commands after any cutover)")
    print("-" * w)
    show(shared)
    for p in (1, 2, 3):
        only = [i for i in f.for_pattern(p) if sorted(i["patterns"]) != [1, 2, 3]]
        print("-" * w)
        print(f"Extra findings for {PATTERN_NAMES[p]}  [{statuses[p]}]")
        print("-" * w)
        show(only)

    print("-" * w)
    print("Before any cutover")
    print("-" * w)
    print(f"  Complete the checklist: {ref('cutover')}")
    print(f"  Review every stop sign: {ref('stops')}")
    print()
    print("Notes: commandstats counts calls since the last stats reset or restart.")
    print("Option-level gaps (for example SET IFEQ) need a code search. The script")
    print("cannot see the options a client sends. Re-run after code changes.")


def recommend(src, statuses):
    order = [1, 2, 3] if src.get("looks_like_cache") else [2, 3, 1]
    for p in order:
        if statuses[p] == "READY":
            return PATTERN_NAMES[p]
    for p in order:
        if statuses[p] != "BLOCKED":
            return PATTERN_NAMES[p] + " (after the listed work)"
    return "None. Every pattern has a stop item. Keep the blocked workload on its current engine."


def wrap(text, width):
    words, lines, cur = text.split(), [], ""
    for wd in words:
        if len(cur) + len(wd) + 1 > width and cur:
            lines.append(cur)
            cur = wd
        else:
            cur = (cur + " " + wd).strip()
    if cur:
        lines.append(cur)
    return lines


def redact(uri):
    u = urlparse(uri)
    if u.password:
        return uri.replace(":" + u.password + "@", ":****@")
    return uri


def main():
    ap = argparse.ArgumentParser(
        description="Read-only assessment of a Redis or Valkey source for migration to Dragonfly. "
                    f"Findings refer to chapters and sections of {GUIDE}.")
    ap.add_argument("source", help="source URI, redis://[user:pw@]host:port[/db] or rediss://...")
    ap.add_argument("target", help="target Dragonfly URI")
    ap.add_argument("--sample", type=int, default=1000,
                    help="number of keys to sample with SCAN + TYPE (default 1000, 0 to skip)")
    ap.add_argument("--json", action="store_true", help="print JSON instead of text")
    ap.add_argument("--insecure", action="store_true", help="skip TLS certificate checks")
    ap.add_argument("--ca", help="CA bundle file for TLS")
    ap.add_argument("--timeout", type=float, default=10.0, help="socket timeout in seconds")
    ap.add_argument("--shake-config", metavar="SHAKE_TOML",
                    help="RedisShake config to check for filter problems (Pattern 3)")
    a = ap.parse_args()

    try:
        sc = Conn(a.source, a.timeout, a.insecure, a.ca)
    except Exception as e:
        print(f"Cannot connect to source {redact(a.source)}: {e}", file=sys.stderr)
        return 3
    try:
        tc = Conn(a.target, a.timeout, a.insecure, a.ca)
    except Exception as e:
        print(f"Cannot connect to target {redact(a.target)}: {e}", file=sys.stderr)
        return 3
    try:
        src = probe_source(sc, a.sample)
        tgt = probe_target(tc, src["commands"])
    finally:
        sc.close()
        tc.close()

    f = assess(src, tgt, a.source, Findings())
    shake_cfg = None
    if a.shake_config:
        try:
            shake_cfg = load_toml(a.shake_config)
            analyze_shake(shake_cfg, src, f, a.shake_config)
        except Exception as e:
            f.add("action", f"Could not read {a.shake_config}", str(e), ["shakefilter"], (3,))
    else:
        f.add("info", "Moving only part of the keyspace?",
              "RedisShake has no key-type filter, and a command-group filter drops snapshot keys. "
              "Re-run with --shake-config shake.toml to check a RedisShake config.", ["shakefilter"], (3,))
    suggest_tool(src, tgt, f, shake_cfg)
    statuses = {p: pattern_status(f.for_pattern(p)) for p in (1, 2, 3)}

    if a.json:
        out = {
            "guide": GUIDE,
            "source": {"uri": redact(a.source), "family": src["family"], "version": src["version_str"],
                       "mode": src["mode"], "keys": src["dbsize"], "types_sampled": src["types"],
                       "commands_used": src["commands"], "modules": src["modules"]},
            "target": {"uri": redact(a.target), "dragonfly": tgt["is_dragonfly"],
                       "version": tgt["version_str"], "keys": tgt["dbsize"]},
            "patterns": {PATTERN_NAMES[p]: {"status": statuses[p], "steps": ref(PATTERN_REF[p]),
                                            "findings": f.for_pattern(p)} for p in (1, 2, 3)},
            "suggested": recommend(src, statuses),
            "suggested_tool": {"tool": src.get("tool_suggestion"), "r2dfly_facts_as_of": R2DFLY_VERSION,
                               "reasons_for_r2dfly": src.get("tool_reasons", []),
                               "reasons_for_redisshake": src.get("tool_shake", []),
                               "r2dfly_limits": src.get("tool_limits", []),
                               "act_before_cutover": src.get("tool_hard_limits", []),
                               "guide": [ref("appb_pick")] + src.get("tool_refs", [])},
            "before_cutover": [ref("cutover"), ref("stops")],
        }
        print(json.dumps(out, indent=2))
    else:
        print_report(src, tgt, a.source, a.target, f)

    vals = list(statuses.values())
    if any(v == "READY" for v in vals):
        return 0
    if all(v == "BLOCKED" for v in vals):
        return 2
    return 1


if __name__ == "__main__":
    sys.exit(main())
