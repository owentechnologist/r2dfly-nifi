package io.dragonfly.nifi.redis.util;

import org.apache.nifi.processor.ProcessSession;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Current values (a lag, a queue depth) published as NiFi counters, which NiFi's Prometheus
 * endpoint exports as {@code nifi_processor_counters}. A counter can only be adjusted, so each
 * report adjusts it by the change since the value this processor last reported. Applied
 * immediately rather than on commit, so a rolled-back session cannot leave the two apart.
 */
public final class CounterGauge {

    private final Map<String, Long> reported = new ConcurrentHashMap<>();

    public void set(ProcessSession session, String name, long value) {
        Long before = reported.put(name, value);
        long delta = value - (before == null ? 0 : before);
        if (delta != 0) {
            session.adjustCounter(name, delta, true);
        }
    }

    public void add(ProcessSession session, String name, long delta) {
        set(session, name, reported.getOrDefault(name, 0L) + delta);
    }
}
