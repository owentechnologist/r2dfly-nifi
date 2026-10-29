# Debugging a stalled or failing migration

If `run-r2dfly.sh`, `simple-migration.sh` or `continuous-migration.sh` starts the flow but the
target's key count isn't growing, or growth stops early, run `diagnose-r2dfly.sh` against the
same NiFi container. It is read-only: it does not stop, start, or reconfigure anything, so it
is safe to run at any point.

All commands below assume you're in the `scripts/` directory (`cd scripts`) - that's where
every `*.sh` file in this project lives.

## Run it

After `simple-migration.sh` or `continuous-migration.sh`, the short way is:

```bash
./simple-troubleshoot.sh
```

It reads the NiFi login and the source/target connection strings that `simple-migration.sh`
saved in the container, runs `diagnose-r2dfly.sh` with them, and adds a short recommendation.

To run the diagnosis directly:

```bash
./diagnose-r2dfly.sh \
  --source-connection-string redis://source-host:6379 \
  --target-connection-string rediss://default:password@target-host:6385
```

The script reads the NiFi login itself: first from the container's logs, then from the copy
`deploy-to-nifi.sh` saved in the container (see "Copy those down" in `TUTORIAL.md`). To use a
different login, pass `--nifi-user`/`--nifi-password` (or `NIFI_USER`/`NIFI_PASS`), or
`--nifi-token`.

The two `--*-connection-string` flags are optional. Pass either or both to check that side
directly (section 4 below); omit one to skip its check. For a cluster, also pass
`--source-connection-mode cluster` or `--target-connection-mode cluster` so DBSIZE is summed
across all masters.

## What it prints

1. **Bulletin board** - the errors NiFi raised (connection failures, auth errors, bad config,
   dead-lettered keys). Start here.
2. **Processor throughput and queue depths** - FlowFiles and bytes in/out for every processor
   in the "R2Dfly Migration" process group, and how many FlowFiles each connection holds. A
   queue stuck at a fixed non-zero count means the processor downstream of it has stalled.
   Zero FlowFiles out from RedisScanReader means it never produced anything.
3. **Controller service state** - enabled/validation status for every controller service in
   the flow, plus any validation errors NiFi is holding.
4. **Direct source/target reachability** - PING and DBSIZE on each side with `redis-cli`,
   bypassing NiFi. It also prints each side's product and version, the modules it supports,
   and warnings for data the target cannot hold. Use this to tell a NiFi-side problem apart
   from a network, auth or TLS problem with the source or target.
5. **Host health** - `docker`/`podman system df`, disk space, and memory. Check this if a
   `podman exec` timeout or an "internal libpod error" appeared anywhere in the migration's
   output: the scripts use `exec` for nearly every check.

## Interpreting the output

- **Bulletins present** -> read them first; they usually name the exact cause (bad
  connection string, TLS handshake failure, auth rejected, etc.).
- **No bulletins, but a queue count is stuck and non-zero** -> the processor downstream of
  that queue can't keep up or can't connect. Check section 3 for its controller service's
  validation state, then section 4 to confirm the target itself is reachable.
- **No bulletins, RedisScanReader shows 0 FlowFiles out** -> the reader isn't running or its
  source connection is failing. Check section 3 for the source controller service, then
  section 4 to confirm the source itself is reachable. If you re-used a Migration ID, the
  scan may already be marked complete (see `TUTORIAL.md`, "Re-running against the same
  source").
- **The flow finished, but the target has fewer keys than the source** -> look in the
  dead-letter directory, `/opt/nifi/nifi-current/r2dfly-dead-letter` inside the NiFi
  container. `RedisDeadLetterWriter` writes one JSON record per failed key, with the reason.
  `simple-migration.sh` and `continuous-migration.sh` print how many records the run added.
  Keys left out by the key-type filter have no record; the run summary counts them.
- **Everything above looks fine, but a `podman exec` timeout showed up earlier in
  `run-r2dfly.sh`'s output** -> check section 5. A flaky container runtime can make earlier
  "enabled/valid" status reads wrong in either direction.

# Starting a migration over from scratch

If diagnosis points to the flow itself being misconfigured beyond a quick fix - e.g. a
connection string that was never reachable from inside the NiFi container (`127.0.0.1` or
`localhost` almost always means this - see `TUTORIAL.md`'s troubleshooting section), or a
migration you just want to abandon and redo cleanly - don't hand-edit the flow. Run
`reset-r2dfly-flow.sh` instead:

```bash
./reset-r2dfly-flow.sh --yes
```

It stops the flow, drains every connection's queued FlowFiles, disables the controller
services, and deletes the whole "R2Dfly Migration" process group (same credential flags as
`diagnose-r2dfly.sh` above). It's the one destructive script here - unlike everything else in
this section, it does not leave your NiFi state untouched, so leave off `--yes` if you want a
confirmation prompt first. Afterward, re-run `./deploy-to-nifi.sh` (or `./simple-migration.sh`,
which calls it automatically) to import a fresh, unconfigured copy of `r2dfly.json`.
