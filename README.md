# r2dfly-nifi
A migration tool from Redis/Valkey to Dragonfly leveraging Apache NiFi

## Project goals:
* Create an easy path for users wanting to migrate
* Support .toml file use in place of/or with commandline args
* Support all keys with values that can be determined
* Support search index migration
* Support Clustered sources and targets
* Allow filtering on keytype and prefix
* Handle large keys
* Copy from RDB and AOF files, and write backups to JSON-lines or RESP files
* Require only normal client read/write access to source and target (the opt-in replication live source also needs `PSYNC` on the source)

[QuickStart](./docs/quickstart.md)

[Tutorial](./docs/TUTORIAL.md)

[debugging](./docs/debugging-help.md)

[r2dfly vs. RedisShake](./docs/r2dfly-vs-redisshake.md)

[Migrating from RedisShake](./docs/migrating-from-redisshake.md)

[Verified behavior](./docs/verified-behavior.md)
