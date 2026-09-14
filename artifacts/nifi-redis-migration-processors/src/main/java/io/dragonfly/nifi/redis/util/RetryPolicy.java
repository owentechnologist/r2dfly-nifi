package io.dragonfly.nifi.redis.util;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with jitter for {@code RedisBatchWriter}'s retry relationship, per spec
 * section 8.1 (100ms initial, x2 multiplier, 30s cap, 5 max attempts, +/-20% jitter).
 *
 * <p>NiFi's {@code ProcessSession.penalize(FlowFile)} only supports one fixed delay per
 * processor, not a per-attempt escalating one, so precise per-attempt timing is implemented via
 * a {@code redis.retry.not-before} FlowFile attribute (epoch millis) computed from this class,
 * which the processor honors with a {@code FlowFileFilter} that skips not-yet-ready FlowFiles
 * when pulling its next batch - the standard NiFi idiom for delayed retry.
 */
public final class RetryPolicy {

    public static final String RETRY_COUNT_ATTRIBUTE = "redis.retry.count";
    public static final String RETRY_NOT_BEFORE_ATTRIBUTE = "redis.retry.not-before";

    private final long initialDelayMs;
    private final double multiplier;
    private final long maxDelayMs;
    private final int maxAttempts;
    private final double jitterFraction;

    private RetryPolicy(long initialDelayMs, double multiplier, long maxDelayMs, int maxAttempts, double jitterFraction) {
        this.initialDelayMs = initialDelayMs;
        this.multiplier = multiplier;
        this.maxDelayMs = maxDelayMs;
        this.maxAttempts = maxAttempts;
        this.jitterFraction = jitterFraction;
    }

    public static RetryPolicy standard() {
        return new RetryPolicy(100L, 2.0, 30_000L, 5, 0.2);
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    /** Delay before retry attempt number {@code attemptNumber} (1-based, i.e. after the 1st failure). */
    public long delayMillisForAttempt(int attemptNumber) {
        double raw = initialDelayMs * Math.pow(multiplier, Math.max(0, attemptNumber - 1));
        double capped = Math.min(raw, maxDelayMs);
        double jitterMultiplier = 1.0 + (ThreadLocalRandom.current().nextDouble() * 2 - 1) * jitterFraction;
        return Math.round(capped * jitterMultiplier);
    }
}
