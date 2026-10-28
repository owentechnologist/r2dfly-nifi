#!/usr/bin/env bash
# Translates a RedisShake 4.x config (shake.toml) into an r2dfly TOML config, and says which
# script to run it with. Settings with no r2dfly equivalent, and places where r2dfly behaves
# differently, are listed as notes on stderr and as comments at the top of the output.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_ROOT/scripts/redis-lib.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") SHAKE_TOML [-o OUTPUT_TOML]
       $(basename "$0") --self-test

Reads a RedisShake config and writes the equivalent r2dfly config to OUTPUT_TOML (default:
stdout). Run the result with the script named in its first comment line, for example:
  ./continuous-migration.sh --toml-file migrate.toml

Translated: the reader (sync_reader, scan_reader, rdb_reader, aof_reader), the writer
(redis_writer, file_writer), address, username, password, tls, cluster, prefer_replica,
dbs/allow_db, allow_key_prefix/block_key_prefix, allow_command_group/block_command_group,
skip_unknown_type, and Lua functions that match RedisShake's documented examples (a prefix
filter, a database filter or move, a command-group filter, a prefix rewrite). Everything else is
listed for review.

--self-test checks the translation against built-in cases and exits non-zero on a mismatch.
EOF
}

INPUT="" OUTPUT="" SELF_TEST="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) OUTPUT="${2:?-o needs a file}"; shift 2 ;;
    --self-test) SELF_TEST="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown argument: $1" >&2; usage; exit 1 ;;
    *) INPUT="$1"; shift ;;
  esac
done
if [[ "$SELF_TEST" != "true" && ! -f "$INPUT" ]]; then
  echo "error: give the RedisShake config to translate (got '${INPUT:-nothing}')" >&2; usage; exit 1
fi

read -r -d '' TRANSLATOR <<'PYEOF' || true
import json, re, sys, tomllib
from urllib.parse import quote

GROUP_TYPES = {"STRING": "string", "BITMAP": "string", "HYPERLOGLOG": "string", "HASH": "hash", "LIST": "list",
               "SET": "set", "SORTED_SET": "zset", "GEO": "zset", "STREAM": "stream"}
CONNECTION = {"address", "username", "password", "tls", "tls_config", "cluster"}
# RedisShake's own option names (internal/reader, internal/writer, internal/filter, internal/config).
# It ignores any other key without a word, so one is reported rather than guessed at.
OPTIONS = {
    "sync_reader": CONNECTION | {"sync_rdb", "sync_aof", "prefer_replica", "try_diskless", "sentinel"},
    "scan_reader": CONNECTION | {"scan", "ksn", "dbs", "prefer_replica", "count", "skip_unknown_type"},
    "rdb_reader": {"filepath"},
    "aof_reader": {"filepath", "timestamp"},
    "redis_writer": CONNECTION | {"off_reply", "sentinel"},
    "file_writer": {"filepath", "type"},
    "filter": {"allow_keys", "allow_key_prefix", "allow_key_suffix", "allow_key_regex", "block_keys", "block_key_prefix",
               "block_key_suffix", "block_key_regex", "allow_db", "block_db", "allow_command", "block_command",
               "allow_command_group", "block_command_group", "allow_slot", "block_slot", "function"},
}
IGNORED = {"advanced": {"dir", "log_file", "log_level", "log_interval", "log_rotation", "log_max_size", "log_max_age",
                        "log_max_backups", "log_compress", "pprof_port", "target_redis_client_max_querybuf_len",
                        "target_redis_proto_max_bulk_len", "target_mbbloom_version"}}
ADVANCED_NOTES = {
    "ncpu": "warning: ncpu has no equivalent; r2dfly's --parallelism and --writer-concurrency default to 1, which was fastest on the benchmark machine",
    "target_redis_max_qps": "warning: target_redis_max_qps has no equivalent; r2dfly limits the scan's load on the source instead (max-source-ops-per-sec, max-source-latency-ms)",
    "pipeline_count_limit": "warning: pipeline_count_limit is not carried over; r2dfly's batch sizes ([migration] batch-size and its per-type forms) keep their defaults",
    "status_port": "warning: status_port has no equivalent; use ./cutover.sh --status and NiFi's Prometheus endpoint",
    "empty_db_before_sync": "manual: empty_db_before_sync has no equivalent; flush the target yourself if it must start empty",
    "aws_psync": "manual: aws_psync is not supported; on ElastiCache use the keyspace-notification live source, with notifications enabled in the parameter group",
}

# Each pattern must match the whole function once comments, shake.log calls and spacing are removed.
LUA = [
    ("prefix_filter", r'local (\w+) = "([^"]*)" local (\w+) = #\1 if not KEYS\[1\] or string\.sub\(KEYS\[1\], 1, \3\) ~= \1 then return end shake\.call\(DB, ARGV\)'),
    ("db_drop", r'if DB == (\d+) then return end shake\.call\(DB, ARGV\)'),
    ("group_drop", r'if GROUP == "([A-Z_]+)" then return end shake\.call\(DB, ARGV\)'),
    ("db_move", r'if DB == (\d+) then DB = (\d+) end shake\.call\(DB, ARGV\)'),
    ("db_swap", r'local (\w+) = (\d+) local (\w+) = (\d+) if DB == \1 then DB = \3 elseif DB == \3 then DB = \1 end shake\.call\(DB, ARGV\)'),
    ("prefix_rewrite", r'local (\w+) = "([^"]*)" local (\w+) = "([^"]*)" for _, (\w+) in ipairs\(KEY_INDEXES\) do local (\w+) = ARGV\[\5\] '
                       r'if \6 and string\.sub\(\6, 1, #\1\) == \1 then ARGV\[\5\] = \3 \.\. string\.sub\(\6, #\1 \+ 1\) end end shake\.call\(DB, ARGV\)'),
]


def normalize_lua(code):
    code = re.sub(r"--[^\n]*", "", code)
    code = re.sub(r"shake\.log\([^\n]*\)", "", code)
    return re.sub(r"\s+", " ", code).strip()


def connection_string(side, notes, label):
    scheme = "rediss" if side.get("tls") else "redis"
    user, password = str(side.get("username", "")), str(side.get("password", ""))
    auth = ""
    if password:
        auth = (quote(user, safe="") if user else "") + ":" + quote(password, safe="") + "@"
        if quote(password, safe="") != password:
            notes.append(f"warning: the {label} password has characters that were URL-encoded in the connection string")
    return f"{scheme}://{auth}{side.get('address', '')}"


def csv(values):
    return ",".join(str(v) for v in values)


def translate(shake):
    notes, out = [], {"source": {}, "target": {}, "migration": {}, "transform": {}}
    readers = [r for r in ("sync_reader", "scan_reader", "rdb_reader", "aof_reader") if r in shake]
    writers = [w for w in ("redis_writer", "file_writer") if w in shake]
    if len(readers) != 1 or len(writers) != 1:
        raise SystemExit(f"error: expected exactly one reader and one writer section, found {readers + writers or 'none'}")
    reader_name, writer_name = readers[0], writers[0]
    reader, writer = shake[reader_name], shake[writer_name]
    source, target = out["source"], out["target"]

    script, live_source = "simple-migration.sh", None
    if reader_name == "sync_reader":
        if reader.get("sync_aof", True):
            script, live_source = "continuous-migration.sh", "replication"
        if not reader.get("sync_rdb", True):
            notes.append("warning: sync_rdb = false skipped RedisShake's snapshot; r2dfly always copies the snapshot first")
        if "try_diskless" in reader:
            notes.append("warning: try_diskless has no equivalent; r2dfly's replication reader skips the RDB and scans keys instead")
    elif reader_name == "scan_reader":
        if reader.get("ksn", False):
            script, live_source = "continuous-migration.sh", "keyspace"
        if reader.get("scan", True) is False:
            notes.append("warning: scan = false skipped RedisShake's scan; r2dfly always copies the snapshot first")
        if reader.get("skip_unknown_type"):
            out["migration"]["disallow-key-types"] = csv(reader["skip_unknown_type"])
    elif reader_name == "rdb_reader":
        source["rdb-file"] = reader.get("filepath", "")
    else:
        source["aof"] = reader.get("filepath", "")
        if reader.get("timestamp", 0):
            notes.append("manual: aof_reader timestamp (replay up to a point in time) has no equivalent; the whole AOF is loaded")

    if reader_name in ("sync_reader", "scan_reader"):
        source["connection-string"] = connection_string(reader, notes, "source")
        if reader.get("cluster"):
            source["connection-mode"] = "cluster"
        if reader.get("prefer_replica"):
            if live_source:
                notes.append("warning: prefer_replica is not carried over; a live phase reads the master, because a replica lags it")
            else:
                source["read-from"] = "replica"
        for key in ("sentinel", "tls_config"):
            if key in reader:
                notes.append(f"manual: [{reader_name}] {key} is not translated; see the quickstart for Sentinel and TLS sources")

    if writer_name == "redis_writer":
        target["connection-string"] = connection_string(writer, notes, "target")
        if writer.get("cluster"):
            target["connection-mode"] = "cluster"
        for key in ("sentinel", "tls_config"):
            if key in writer:
                notes.append(f"manual: [redis_writer] {key} is not translated; see the quickstart for TLS targets")
    else:
        if live_source:
            notes.append("warning: r2dfly writes files only in a one-time copy, so this config runs simple-migration.sh and has no live phase")
            script, live_source = "simple-migration.sh", None
        target["file"] = writer.get("filepath", "")
        target["file-format"] = "jsonl" if writer.get("type", "cmd") == "json" else "resp"

    flt = shake.get("filter", {})
    if flt.get("allow_key_prefix"):
        out["migration"]["prefix-only-list"] = csv(flt["allow_key_prefix"])
    if flt.get("block_key_prefix"):
        out["migration"]["prefix-deny-list"] = csv(flt["block_key_prefix"])
    for key in ("allow_keys", "block_keys", "allow_key_suffix", "block_key_suffix", "allow_key_regex", "block_key_regex",
                "allow_command", "block_command", "allow_slot", "block_slot"):
        if flt.get(key):
            notes.append(f"manual: [filter] {key} has no equivalent; r2dfly filters keys by prefix and type only")
    translate_groups(flt, out, notes, live_source)

    dbs = list(reader.get("dbs") or []) or list(flt.get("allow_db") or [])
    target_db = None
    if flt.get("function"):
        dbs, target_db = translate_function(flt["function"], dbs, out, notes)
    if flt.get("block_db"):
        notes.append(f"manual: block_db {flt['block_db']} has no equivalent; r2dfly copies one database per run, so run once per database you want")
    if len(dbs) > 1:
        notes.append(f"manual: RedisShake copied databases {dbs}; r2dfly copies one database per run, so this config covers {dbs[0]} - run it again with /<db> at the end of both connection strings for the others")
    if dbs and "connection-string" in source:
        source["connection-string"] += f"/{dbs[0]}"
        if "connection-string" in target:
            target["connection-string"] += f"/{dbs[0] if target_db is None else target_db}"
    elif not dbs and reader_name in ("sync_reader", "scan_reader") and not reader.get("cluster"):
        notes.append("warning: RedisShake copied every database; this config copies database 0 - run it again with /<db> at the end of both connection strings for each other database you use")

    adv = shake.get("advanced", {})
    behavior = adv.get("rdb_restore_command_behavior")
    if behavior in ("skip", "panic"):
        notes.append(f"manual: rdb_restore_command_behavior = \"{behavior}\" is RedisBatchWriter's conflict-strategy {'SKIP' if behavior == 'skip' else 'FAIL'}, set on the processor in NiFi; r2dfly overwrites by default, and FAIL dead-letters the key instead of stopping")
    for key in adv:
        if key in ADVANCED_NOTES:
            notes.append(ADVANCED_NOTES[key])
        elif key != "rdb_restore_command_behavior" and key not in IGNORED["advanced"]:
            notes.append(f"manual: [advanced] {key} is not translated")
    for section, values in shake.items():
        if section in OPTIONS:
            for key in values:
                if key not in OPTIONS[section]:
                    notes.append(f"warning: [{section}] {key} is not a RedisShake option, so RedisShake ignored it; it is not translated")
        elif section not in ("advanced", "module"):
            notes.append(f"manual: [{section}] is not translated")

    if live_source:
        out["live"] = {"source": live_source}
    return script, {k: v for k, v in out.items() if v}, notes


def translate_groups(flt, out, notes, live_source):
    allow, block = [g.upper() for g in flt.get("allow_command_group", [])], [g.upper() for g in flt.get("block_command_group", [])]
    if allow:
        unknown = [g for g in allow if g not in GROUP_TYPES and g != "GENERIC"]
        if "GENERIC" in allow:
            notes.append("warning: allow_command_group includes GENERIC, so RedisShake copied every snapshot key with RESTORE whatever its type; this config does the same and copies every type")
        else:
            out["migration"]["key-types"] = csv(sorted({GROUP_TYPES[g] for g in allow if g in GROUP_TYPES}))
            notes.append("warning: the command-group trap. RedisShake writes each snapshot key with RESTORE, which is in the GENERIC group, so "
                         f"allow_command_group = {allow} copied no snapshot keys" + (" and only the live writes of those groups" if live_source == "replication" else "")
                         + ". r2dfly reads each key's type, so this config copies those types' keys in the snapshot too")
        if unknown:
            notes.append(f"warning: allow_command_group {unknown} names no data type and is dropped")
    if block:
        if "GENERIC" in block:
            notes.append("warning: block_command_group GENERIC also blocked RedisShake's snapshot RESTOREs, deletes and expiries; r2dfly copies them, and that part is not carried over")
        types = sorted({GROUP_TYPES[g] for g in block if g in GROUP_TYPES})
        if types:
            existing = out["migration"].get("disallow-key-types")
            out["migration"]["disallow-key-types"] = csv(([existing] if existing else []) + types)
            notes.append(f"warning: block_command_group {block} let RedisShake's snapshot copy those types anyway, with RESTORE; r2dfly skips their keys in the snapshot too")


def translate_function(code, dbs, out, notes):
    """Returns the source databases and the target database the function implies."""
    body = normalize_lua(code)
    for name, pattern in LUA:
        m = re.fullmatch(pattern, body)
        if not m:
            continue
        if name == "prefix_filter":
            out["migration"]["prefix-only-list"] = m.group(2)
        elif name == "group_drop":
            if m.group(1) in GROUP_TYPES:
                out["migration"]["disallow-key-types"] = GROUP_TYPES[m.group(1)]
            else:
                notes.append(f"warning: the function dropped GROUP {m.group(1)}, which is no data type; r2dfly copies keys, not those commands, so nothing to translate")
        elif name == "prefix_rewrite":
            out["transform"]["rename-prefix"] = f"{m.group(2)}={m.group(4)}"
        elif name == "db_drop":
            notes.append(f"manual: the function dropped database {m.group(1)}; r2dfly copies one database per run, so just do not run it for {m.group(1)}")
        elif name == "db_move":
            return [int(m.group(1))], int(m.group(2))
        elif name == "db_swap":
            a, b = int(m.group(2)), int(m.group(4))
            notes.append(f"manual: the function swapped databases {a} and {b}; this config copies {a} into {b} - run it again from /{b} to /{a}")
            return [a], b
        return dbs, None
    notes.append("manual: the Lua function matches none of RedisShake's documented examples and is not translated; port it to a Groovy --transform-script, or to the built-in transforms:\n" + code.strip())
    return dbs, None


def render(script, settings, notes, source_name):
    lines = [f"# Run with: ./{script} --toml-file <this file>", f"# Translated from {source_name} by import-shake.sh."]
    if notes:
        lines.append("# Review these before running:")
        for note in notes:
            lines.extend("#   " + line for line in ("- " + note).splitlines())
    for section, values in settings.items():
        lines.append("")
        lines.append(f"[{section}]")
        for key, value in values.items():
            lines.append(f"{key} = {json.dumps(value)}")
    return "\n".join(lines) + "\n"


CASES = [
    ("sync to continuous replication, cluster and auth",
     '[sync_reader]\naddress = "10.0.0.1:6379"\ncluster = true\npassword = "p@ss"\n[redis_writer]\naddress = "df:6379"\ntls = true\n',
     "continuous-migration.sh",
     {"source": {"connection-string": "redis://:p%40ss@10.0.0.1:6379", "connection-mode": "cluster"},
      "target": {"connection-string": "rediss://df:6379"}, "live": {"source": "replication"}},
     ["URL-encoded"]),
    ("scan without ksn is a one-time copy; skip_unknown_type and prefixes",
     '[scan_reader]\naddress = "r:6379"\nskip_unknown_type = ["ReJSON-RL"]\n[redis_writer]\naddress = "df:6379"\n[filter]\nallow_key_prefix = ["user:", "order:"]\nblock_key_prefix = ["tmp:"]\n',
     "simple-migration.sh",
     {"source": {"connection-string": "redis://r:6379"}, "target": {"connection-string": "redis://df:6379"},
      "migration": {"disallow-key-types": "ReJSON-RL", "prefix-only-list": "user:,order:", "prefix-deny-list": "tmp:"}},
     ["every database"]),
    ("the command-group trap becomes a real type filter",
     '[scan_reader]\naddress = "r:6379"\nksn = true\ndbs = [3]\n[redis_writer]\naddress = "df:6379"\n[filter]\nallow_command_group = ["HASH", "SORTED_SET"]\n',
     "continuous-migration.sh",
     {"source": {"connection-string": "redis://r:6379/3"}, "target": {"connection-string": "redis://df:6379/3"},
      "migration": {"key-types": "hash,zset"}, "live": {"source": "keyspace"}},
     ["command-group trap", "copied no snapshot keys"]),
    ("documented prefix rewrite",
     '[sync_reader]\naddress = "r:6379"\nsync_aof = false\n[redis_writer]\naddress = "df:6379"\n[filter]\nfunction = """\nlocal prefix_old = "old_"\nlocal prefix_new = "new_"\nshake.log("old=" .. table.concat(ARGV, " "))\nfor _, index in ipairs(KEY_INDEXES) do\n  local key = ARGV[index]\n  if key and string.sub(key, 1, #prefix_old) == prefix_old then\n    ARGV[index] = prefix_new .. string.sub(key, #prefix_old + 1)\n  end\nend\nshake.call(DB, ARGV)\n"""\n',
     "simple-migration.sh",
     {"source": {"connection-string": "redis://r:6379"}, "target": {"connection-string": "redis://df:6379"},
      "transform": {"rename-prefix": "old_=new_"}},
     []),
    ("database move and group drop by function",
     '[sync_reader]\naddress = "r:6379"\n[redis_writer]\naddress = "df:6379"\n[filter]\nfunction = """\nif DB == 1\nthen\n  DB = 2\nend\nshake.call(DB, ARGV)\n"""\n',
     "continuous-migration.sh",
     {"source": {"connection-string": "redis://r:6379/1"}, "target": {"connection-string": "redis://df:6379/2"},
      "live": {"source": "replication"}},
     []),
    ("an unrecognized function is listed for review",
     '[rdb_reader]\nfilepath = "/data/dump.rdb"\n[file_writer]\nfilepath = "/out.jsonl"\ntype = "json"\n[filter]\nfunction = "shake.call(DB, {\\"SET\\", KEYS[1], \\"x\\"})"\n[advanced]\nrdb_restore_command_behavior = "skip"\nlog_level = "debug"\n',
     "simple-migration.sh",
     {"source": {"rdb-file": "/data/dump.rdb"}, "target": {"file": "/out.jsonl", "file-format": "jsonl"}},
     ["matches none", "conflict-strategy SKIP"]),
    ("a key RedisShake does not know is reported, not dropped",
     '[sync_reader]\naddress = "r:6379"\nsync_aof = false\n[redis_writer]\naddress = "df:6379"\n[filter]\nblock_key_type = ["ReJSON-RL"]\n',
     "simple-migration.sh",
     {"source": {"connection-string": "redis://r:6379"}, "target": {"connection-string": "redis://df:6379"}},
     ["block_key_type is not a RedisShake option"]),
]


def self_test():
    failed = 0
    for name, text, script, settings, note_parts in CASES:
        got_script, got_settings, notes = translate(tomllib.loads(text))
        problems = []
        if got_script != script:
            problems.append(f"script {got_script!r}, expected {script!r}")
        if got_settings != settings:
            problems.append(f"settings {got_settings}, expected {settings}")
        for part in note_parts:
            if not any(part in n for n in notes):
                problems.append(f"no note mentions {part!r}: {notes}")
        print(("FAIL " if problems else "ok   ") + name)
        for p in problems:
            print("     " + p)
        failed += bool(problems)
    print(f"{len(CASES) - failed} of {len(CASES)} cases passed")
    sys.exit(1 if failed else 0)


if sys.argv[1:] == ["--self-test"]:
    self_test()
script, settings, notes = translate(tomllib.load(sys.stdin.buffer))
for note in notes:
    print("note: " + note.splitlines()[0], file=sys.stderr)
sys.stdout.write(render(script, settings, notes, sys.argv[1] if len(sys.argv) > 1 else "shake.toml"))
PYEOF

redis_lib_pick_runtime
if [[ "$SELF_TEST" == "true" ]]; then
  toolbox_run python3 -c "$TRANSLATOR" --self-test
  exit $?
fi
if [[ -n "$OUTPUT" ]]; then
  toolbox_run python3 -c "$TRANSLATOR" "$(basename "$INPUT")" < "$INPUT" > "$OUTPUT"
  echo "wrote $OUTPUT; run it with the script named on its first line" >&2
else
  toolbox_run python3 -c "$TRANSLATOR" "$(basename "$INPUT")" < "$INPUT"
fi
