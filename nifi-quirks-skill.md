# NiFi Operational Quirks (lessons learned building this project)

> Hard-won lessons from building and operating the NiFi-based Redis -> Dragonfly migration
> flow in this project (NiFi 2.11.0, run via podman/docker, custom processors in
> `nifi-redis-migration-nar`). Give this file to Claude (or any LLM) as context before making
> further changes to `run-r2dfly.sh`, `simple-migration.sh`, `deploy-to-nifi.sh`, or the
> processor Java code - several of these bugs are easy to reintroduce because they fail
> silently (no error, no warning, just wrong or missing data).

## 1. REST API property updates are a partial merge, not a full replace

`PUT /processors/{id}` and `PUT /controller-services/{id}` only update the property keys you
actually include in the JSON body - any key you omit keeps whatever value a **previous**
call set, it is not reset to empty/default. This bit us badly: a script that only included
`prefix-only-list`/`prefix-deny-list` in the properties map when the user passed the
corresponding flag left a stale value from an unrelated earlier test in place on every run
that didn't pass the flag. The processor scanned correctly and emitted zero errors - it was
silently filtering out every key via the leftover prefix list. **Rule: always include every
optional property key on every PUT, using JSON `null` (not an omitted key, and not empty
string - many property validators reject empty string) to explicitly mean "unset".**

## 2. Controller services reject property updates while enabled

`PUT` to an **enabled** controller service fails with a plain-text (not JSON) body:
`"Cannot modify configuration of ... because it is currently not disabled"`. If your code
discards the PUT response (e.g. redirects to `/dev/null`) this fails completely silently -
the service keeps running with its old configuration forever, while every other signal
(processor state, connection queues, logs) looks perfectly healthy. This exact bug made
every migration run after the first one silently write to the *original* target instead of
whatever `--target-connection-string` a later run specified - real network connectivity,
TLS, and scanning were all fine, data just landed in the old place. **Rule: always
disable a controller service (`PUT .../run-status` with `state: "DISABLED"`) before PUTting
its properties, and always parse the PUT response - treat a non-JSON or error response as a
hard failure, not something to swallow.**

## 3. Processor/service run-status changes are asynchronous

`PUT /processors/{id}/run-status` and `PUT /controller-services/{id}/run-status` return
before the component has actually finished transitioning state. Code that immediately
proceeds to reconfigure a processor/service right after telling it to stop/disable can race
the actual transition. Poll the component's own status field until it reports the expected
state before proceeding.

## 4. The NAR auto-loader does not hot-reload an unchanged Maven coordinate

NiFi's `nar_extensions/` autoload directory keys on `groupId:artifactId:version`. If you keep
your dev NAR pinned at `1.0.0-SNAPSHOT` (as this project does), copying a rebuilt `.nar` file
into that directory on a *running* container does nothing - `nifi-app.log` will show
`Found existing bundle with coordinate ..., will not load`, and the old compiled code keeps
running. A full container restart is required to pick up the change (this also conveniently
resets the in-memory `MapCacheServer` state, clearing any stale migration-id checkpoints).
`deploy-to-nifi.sh` automates this: it restarts the container automatically whenever it
reuses an already-running one, and skips the restart only for a genuinely fresh container
that has nothing loaded yet.

## 5. `mvn -pl <processors-module> -am package` does not rebuild the NAR

Building just the processors module (even with `-am` to include its dependencies) does
**not** rebuild the downstream NAR-assembly module. This silently leaves a stale `.nar` file
that then gets faithfully redeployed by whatever deploy step you have - no error, just old
code. Build the NAR module itself, or the whole reactor with no `-pl` filter.

## 6. `nifi-app.log` is your first stop, always

`/opt/nifi/nifi-current/logs/nifi-app.log` inside the container is readable without any
credentials (`podman exec <container> grep/sed/tail ...`) and has full stack traces for
processor exceptions. Check it before guessing at a live-flow failure's cause - several bugs
in this project were found in minutes once someone actually read this file, after much longer
spent forming theories from the outside.

**Other diagnostic techniques that worked, roughly in order of how far they got before the
real answer:**
1. `nifi-app.log` (above).
2. A JVM thread dump (`jstack <nifi-pid>`, JDK is at `$JAVA_HOME` in the container) shows
   exactly what's currently executing - good for ruling things out, but can miss a
   fast-cycling yield loop between snapshots.
3. Temporary `getLogger().info(...)` calls at key decision points in the processor code, then
   rebuild+redeploy+restart (see #4/#5 above) - the most reliable way to see what a fast,
   silent-on-both-success-and-normal-yield processor is actually doing. Don't spend too long
   on log-archaeology or thread dumps before just adding a log line.
4. Compare `flowFilesIn`/`flowFilesOut`/queue sizes across the whole processor chain
   (`GET .../processors` and `.../connections`) to pinpoint which stage actually has the data.
5. If everything upstream reports success and data still isn't at the expected destination,
   check whether it landed somewhere *else* (e.g. a stale prior target) before assuming a
   deeper protocol/network bug - see #2 above, the "boring" explanation won out over several
   more exotic theories that were tested and ruled out first.

## 7. Connection status fields are strings, not numbers - and comma-formatted once large

`GET /process-groups/{id}/connections`'s `status.aggregateSnapshot.queuedCount` (and similar
formatted-count fields) come back as JSON **strings**, not integers - summing them directly
(`sum(c['status']['aggregateSnapshot']['queuedCount'] for c in ...)` in Python) throws
`TypeError: unsupported operand type(s) for +: 'int' and 'str'` as soon as more than one
connection is involved. A bare `int(...)` cast looks like it fixes this, and does for small
counts (e.g. `"0"`) - but NiFi comma-groups this same field once the count is large enough
(e.g. `"6,930"`), and `int("6,930")` itself raises `ValueError: invalid literal for int() with
base 10: '6,930'`. Always strip separators first: `int(s.replace(',', ''))`. (The sibling
`status.aggregateSnapshot.queued` field - `"6,930 (10.5 MB)"` - is comma-formatted the same
way, but that one's display-only in practice here, never parsed back to a number.)

## 8. Clearing a connection's queue is asynchronous

`POST /flowfile-queues/{connection-id}/drop-requests` creates a drop request and returns
immediately with a request id - it does not clear the queue synchronously. Poll
`GET /flowfile-queues/{connection-id}/drop-requests/{id}` until `finished` is true.

## 9. NiFi's HTTPS listener binds to the container's own hostname, not `localhost`

Any CLI/curl/API call made from **inside** the container's network namespace (e.g. via
`podman exec`) must target `https://$(podman exec <container> hostname):8443`, not
`localhost:8443` - the listener doesn't accept the loopback name from inside its own
container in this setup. From the **host** machine, `localhost:8443` (the published port)
works fine. Don't assume the same URL works from both places.

## 10. Connection strings given to processors must resolve inside NiFi's own network, not the host's

A `localhost`-based Redis/Dragonfly connection string works fine for a script running
directly on the host (e.g. for `DBSIZE` polling), but the *actual NiFi processors* run inside
their own container with their own network namespace - passing them a `localhost` connection
string makes them try to connect to `localhost` from inside their own container, which fails
with `Connection refused`. Meanwhile the host-side polling using the same URL happily
succeeds, since it isn't going through the flow at all - so the flow can look perfectly
healthy (VALID, ENABLED, no errors) while silently moving zero data. Use container-network
DNS names (e.g. `redis://source-redis:6379`) reachable from inside the NiFi container, not
host-published ports/`localhost`, for any connection string that a processor will actually
use.

Related: a container's published host port and its actual internal listening port can
legitimately differ (e.g. `podman port` shows `6385 -> 6385` but the process only ever logs
`listening on 0.0.0.0:6379`) - in that case nothing is actually listening on the container's
own 6385, so a rootless proxy accepts host connections to 6385 and then immediately closes
them ("Server closed the connection"), which looks different from a clean connection refusal
and can be confusing. When in doubt, `podman exec <container> redis-cli ...` (using the
container's real internal port) is the ground truth.

## 11. Bundle/coordinate discovery: use the flow-types endpoints, not `list-nars`

To find a processor's or controller service's exact bundle coordinate (`group:artifact:
version`) for building REST API request bodies from scratch, use
`GET /nifi-api/flow/controller-service-types` and `GET /nifi-api/flow/processor-types` - the
CLI's `list-nars` only shows NARs added through the runtime NAR Manager, not ones loaded via
the static `nar_extensions/` autoload directory that this project (and many custom-processor
setups) uses.

## 12. The NiFi CLI toolkit can't create processors/connections from scratch

`/opt/nifi/nifi-toolkit-current/bin/cli.sh` (inside the container) has no "create processor"
or "create connection" command - building a flow programmatically from nothing requires the
raw REST API (`POST .../processors`, `.../controller-services`, `.../connections`, wiring
them together manually). It **does** support importing/exporting an already-built flow
directly from/to a local JSON file (`pg-import -i <file>`, `pg-export -pgid <id> -o <file>`)
with no NiFi Registry required - build the flow once via the REST API (or the UI), export it,
and import that file on every future setup instead of re-building it via API calls each time.
`pg-export` also automatically strips out properties marked `sensitive: true` (e.g.
connection strings) - this is what makes a "check out this file, only configure two
properties" onboarding flow work without any manual sanitizing step.

## 13. Standard framework controller services name properties differently than custom ones

Built-in NiFi framework services like `MapCacheServer`/`MapCacheClientService` expose
properties where the API's `name` field equals the human-readable `displayName` (e.g.
`"Server Hostname"`). This project's own custom controller services use kebab-case internal
names (e.g. `connection-string`) that are distinct from their display names. Don't assume one
convention when writing generic property-lookup code against an arbitrary controller service
- check which kind you're dealing with.

## 14. TLS controller services need *some* keystore/truststore, even to "just trust public CAs"

`StandardSSLContextService` won't accept a bare "trust the system CAs" configuration with no
keystore/truststore at all. Point both its keystore and truststore at the NiFi container's own
JVM default truststore (`$JAVA_HOME/lib/security/cacerts`, type PKCS12, default password
`changeit`) - it already trusts publicly-CA-signed endpoints (e.g. a cloud database's TLS
cert) without needing a client certificate. This satisfies the service's validation
requirements while achieving "just trust the usual CAs."

Also: when exposing a "require TLS" option as a CLI flag, make it a bare on/off switch (its
mere presence means true), not a flag that consumes a value - a `--source-require-tls`/
`--target-require-tls` style flag that was originally wired to expect `--flag value` crashed
under `set -u` the first time someone used it as a bare switch, which is the natural way such
a flag reads.

## 15. Custom checkpoint state can make a re-run look "stuck" when it's actually just skipping

If a processor checkpoints per-partition progress keyed by a value your tooling lets the user
reuse across runs (e.g. a "migration id"), re-running with the same id after a prior
successful run makes it yield immediately without doing any new work - which looks exactly
like a hang from the outside (no progress, no errors, no obvious feedback). If your tooling
generates such an id automatically, always generate a fresh one by default per run (e.g.
timestamp-suffixed) rather than reusing a fixed default.
