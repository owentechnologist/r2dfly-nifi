package io.dragonfly.nifi.redis.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class Futures {

    private Futures() {
    }

    /**
     * Waits until every future has completed, successfully or not, or the timeout passes; the
     * caller then reads each one. LettuceFutures.awaitAll stops at the first failed reply, so the
     * replies still in flight behind it looked like timeouts.
     */
    public static void awaitAll(Collection<? extends CompletionStage<?>> futures, long timeoutMs) {
        try {
            CompletableFuture.allOf(futures.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new))
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            // read per future by the caller
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void awaitAll(CompletionStage<?>[] futures, long timeoutMs) {
        awaitAll(Arrays.asList(futures), timeoutMs);
    }
}
