# Debugging a stalled or failing migration

If `run-r2dfly.sh` (or `simple-migration.sh`) starts the flow but the target's key count
isn't growing, or growth stops early, run `diagnose-r2dfly.sh` against the same NiFi
container. It's read-only - it won't stop, start, or reconfigure anything - so it's safe to
run at any point.

All commands below assume you're in the `scripts/` directory (`cd scripts`) - that's where
every `*.sh` file in this project lives.

## Run it

```bash
NIFI_USER=<user> NIFI_PASS=<password> ./diagnose-r2dfly.sh \
  --source-connection-string redis://source-host:6379 \
  --target-connection-string rediss://default:password@target-host:6385
```

`NIFI_USER`/`NIFI_PASS` are the single-user credentials printed once in the container's
logs at first creation - see `deploy-to-nifi.sh`'s output, or `podman logs
nifi-redis-migration | grep -A1 'Generated Username'`. You can also pass `--nifi-token`
instead if you already have a bearer token.

The two `--*-connection-string` flags are optional - pass either or both to also PING/DBSIZE
that side directly (see section 4 below); omit one to skip its check.

## What it prints

1. **Bulletin board** - actual exceptions NiFi raised (connection failures, auth errors,
   bad config, etc.). Start here.
2. **Processor throughput and queue depths** - flowfiles in/out for RedisScanReader,
   RedisBatchWriter, ModuleTypeHandler, and RedisTypeDeserializer, plus how many flowfiles
   are queued between them. A queue stuck at a fixed non-zero count means whatever's
   downstream of it has stalled; all-zero counts on the reader means it never started
   producing at all.
3. **Controller service state** - enabled/validation status for every controller service in
   the flow, plus any validation errors NiFi is holding.
4. **Direct source/target reachability** - PINGs and DBSIZEs each side directly with
   `redis-cli`, bypassing NiFi entirely. Use this to tell a NiFi-side bug apart from a plain
   network/auth/TLS problem with the source or target.
5. **Podman host health** - `system df`, disk space, and memory. Worth checking if you've
   seen a `podman exec` timeout or an "internal libpod error" anywhere in the migration's
   output, since the whole toolchain leans on `podman exec` for nearly every check.

## Interpreting the output

- **Bulletins present** -> read them first; they usually name the exact cause (bad
  connection string, TLS handshake failure, auth rejected, etc.).
- **No bulletins, but target queue count is stuck and non-zero** -> the writer/service
  downstream of that queue can't keep up or can't connect - check section 3 for that
  service's validation state, then section 4 to confirm the target itself is reachable.
- **No bulletins, RedisScanReader shows 0 flowfiles out** -> the reader isn't running or its
  source connection is failing - check section 3 for the source controller service, then
  section 4 to confirm the source itself is reachable.
- **Everything above looks fine, but a `podman exec` timeout showed up earlier in
  `run-r2dfly.sh`'s output** -> check section 5; a flaky container runtime can make earlier
  "enabled/valid" status reads unreliable even when the flow's actual state is fine (or vice
  versa).

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
