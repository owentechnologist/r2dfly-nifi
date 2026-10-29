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

[Quickstart](./docs/quickstart.md)

[Tutorial](./docs/TUTORIAL.md)

[Debugging](./docs/debugging-help.md)

[r2dfly vs. RedisShake](./docs/r2dfly-vs-redisshake.md)

[Migrating from RedisShake](./docs/migrating-from-redisshake.md)

[Verified behavior](./docs/verified-behavior.md)

[Project status, for maintainers](./docs/20261028-status.md)
