package io.dragonfly.nifi.redis.services;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

@Tags({"dragonfly", "dragonflydb", "migration", "connection", "target"})
@CapabilityDescription("Provides a pooled connection to a target DragonflyDB instance for use "
        + "by the migration write processors. Kept as a distinct controller service type from "
        + "the source RedisConnectionPoolService so credentials and provenance are attributed "
        + "independently, even though Dragonfly speaks the same protocol as Redis.")
public class StandardDragonflyConnectionPoolService extends AbstractRedisConnectionPoolService
        implements DragonflyConnectionPoolService {
}
