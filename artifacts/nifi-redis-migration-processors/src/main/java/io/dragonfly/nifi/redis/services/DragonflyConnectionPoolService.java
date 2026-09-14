package io.dragonfly.nifi.redis.services;

/**
 * Identical contract to {@link RedisConnectionPoolService}, kept as a distinct controller
 * service type so migration source and target credentials are configured, secured, and
 * attributed in provenance independently of one another.
 */
public interface DragonflyConnectionPoolService extends RedisConnectionPoolService {
}
