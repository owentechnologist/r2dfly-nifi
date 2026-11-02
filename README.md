# r2dfly-nifi
Migrates data from Redis or Valkey to Dragonfly. It runs on Apache NiFi, driven by the scripts in `scripts/`.

## What it does
* Copies a source once (`simple-migration.sh`), or once with no NiFi server (`headless-migration.sh`)
* Keeps the target current until cutover (`continuous-migration.sh`), then checks that it is safe to switch (`cutover.sh`)
* Takes settings from a TOML file, from command-line flags, or both
* Translates a RedisShake `shake.toml` into an r2dfly TOML file (`import-shake.sh`)
* Copies every core type, stream consumer groups, hash field TTLs, JSON and TopK keys, and search indexes. A key it cannot copy gets a dead-letter record with the reason
* Reads and writes standalone, Sentinel and cluster deployments
* Filters by key type and prefix; renames prefixes, remaps databases, changes TTLs, and runs a Groovy hook on each key
* Splits large keys into chunks
* Copies binary key names and values byte for byte
* Reads RDB and AOF files, and writes backups as JSON-lines or RESP files
* Needs only normal client read and write access to source and target. The default live source also needs keyspace notifications enabled on the source; the opt-in replication live source needs `PSYNC`

## Get it
Docker or Podman is the only thing the host needs. Download a release from the
[Releases page](https://github.com/owentechnologist/r2dfly-nifi/releases), check it, and unzip it:

```bash
V=r2dfly-A-013   # the release you want
curl -LO https://github.com/owentechnologist/r2dfly-nifi/releases/download/$V/$V.zip
curl -LO https://github.com/owentechnologist/r2dfly-nifi/releases/download/$V/$V.zip.sha256
shasum -a 256 -c $V.zip.sha256   # or: sha256sum -c $V.zip.sha256
unzip $V.zip && cd $V/scripts
./simple-migration.sh \
  --source-connection-string redis://source-host:6379 \
  --target-connection-string rediss://default:password@target-host:6385
```

A release includes the built processor NAR, so nothing is compiled on your host. A clone of
this repository works too: its first run builds the NAR in a Maven container.

Maintainers: bump `R2DFLY_VERSION` in `scripts/version.sh`, commit and push, then run
`./build-release.sh --publish`.

## Docs
[Quickstart](./docs/quickstart.md)

[Tutorial](./docs/TUTORIAL.md)

[Debugging](./docs/debugging-help.md)

[r2dfly vs. RedisShake](./docs/r2dfly-vs-redisshake.md)

[Migrating from RedisShake](./docs/migrating-from-redisshake.md)

[Verified behavior](./docs/verified-behavior.md)

[Project status, for maintainers](./docs/20261028-status.md)
