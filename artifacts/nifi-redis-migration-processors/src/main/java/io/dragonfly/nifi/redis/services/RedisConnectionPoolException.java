package io.dragonfly.nifi.redis.services;

/** Thrown when a pooled Redis/Dragonfly connection cannot be borrowed or established. */
public class RedisConnectionPoolException extends RuntimeException {

    public RedisConnectionPoolException(String message, Throwable cause) {
        super(message, cause);
    }
}
