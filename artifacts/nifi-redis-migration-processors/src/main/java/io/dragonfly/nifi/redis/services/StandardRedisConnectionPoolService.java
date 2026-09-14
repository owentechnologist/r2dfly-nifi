package io.dragonfly.nifi.redis.services;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

@Tags({"redis", "valkey", "migration", "connection", "source"})
@CapabilityDescription("Provides a pooled connection to a source Redis-compatible datastore "
        + "(Redis OSS, Redis Stack, Valkey, ElastiCache) for use by the migration scan and "
        + "live-phase processors.")
public class StandardRedisConnectionPoolService extends AbstractRedisConnectionPoolService
        implements RedisConnectionPoolService {
}
