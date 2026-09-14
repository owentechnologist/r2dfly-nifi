package io.dragonfly.nifi.redis.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryPolicyTest {

    @Test
    void maxAttemptsMatchesSpec() {
        assertEquals(5, RetryPolicy.standard().maxAttempts());
    }

    @Test
    void delayGrowsExponentiallyWithinJitterBounds() {
        RetryPolicy policy = RetryPolicy.standard();
        // attempt 1 -> ~100ms, attempt 2 -> ~200ms, +/-20% jitter each.
        long attempt1 = policy.delayMillisForAttempt(1);
        long attempt2 = policy.delayMillisForAttempt(2);
        assertTrue(attempt1 >= 80 && attempt1 <= 120, "attempt 1 delay out of expected range: " + attempt1);
        assertTrue(attempt2 >= 160 && attempt2 <= 240, "attempt 2 delay out of expected range: " + attempt2);
    }

    @Test
    void delayIsCappedAtMaxDelay() {
        RetryPolicy policy = RetryPolicy.standard();
        long farAttempt = policy.delayMillisForAttempt(20);
        // cap is 30s +/-20% jitter.
        assertTrue(farAttempt <= 36_000, "delay should be capped near 30s, was " + farAttempt);
    }
}
