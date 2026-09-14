package io.dragonfly.nifi.redis.util;

/** Thrown when {@link RedisValueFetcher} fails to read a key's value from the source. */
public class RedisFetchException extends RuntimeException {

    public RedisFetchException(String message, Throwable cause) {
        super(message, cause);
    }
}
