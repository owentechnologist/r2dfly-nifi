package io.dragonfly.nifi.redis.util;

import com.fasterxml.jackson.databind.JsonNode;
import io.dragonfly.nifi.redis.services.RedisConnectionPoolService;
import io.lettuce.core.RestoreArgs;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Decides, once per (type, encoding), whether the target restores the source's DUMP payloads
 * faithfully. The first key of each pair is DUMPed from the source, RESTOREd to a scratch key on
 * the target, and both sides are read back with the typed fetch and compared. The source is only
 * read. Payload compatibility depends on the source version and on the encoding (Dragonfly 2.0.2
 * accepts every core type from Redis 7.x and Valkey 7.2/8.1, rejects everything from Redis 8 and
 * Valkey 9, and rejects Redis 7.4 hashes with field TTLs), so neither can be assumed up front.
 */
public final class RestoreProbe {

    public record Verdict(boolean accepted, String detail) {
    }

    private final RedisConnectionPoolService source;
    private final RedisConnectionPoolService target;
    private final Map<String, Verdict> verdicts = new ConcurrentHashMap<>();

    public RestoreProbe(RedisConnectionPoolService source, RedisConnectionPoolService target) {
        this.source = source;
        this.target = target;
    }

    /** Returns the cached verdict for this pair, probing with {@code sampleKey} the first time. */
    public Verdict verdictFor(byte[] sampleKey, String type, String encoding, VerdictListener listener) {
        String pair = type + "/" + (encoding == null ? "unknown" : encoding);
        return verdicts.computeIfAbsent(pair, p -> {
            Verdict verdict = probe(sampleKey, type);
            listener.onVerdict(p, RedisBytes.display(sampleKey), verdict);
            return verdict;
        });
    }

    @FunctionalInterface
    public interface VerdictListener {
        void onVerdict(String typeAndEncoding, String sampleKey, Verdict verdict);
    }

    private Verdict probe(byte[] rawKey, String type) {
        byte[] payload;
        Optional<List<KeyRecord>> expected;
        try {
            payload = source.withConnection(cmds -> await(() -> cmds.dump(rawKey).get()));
            expected = source.withConnection(cmds -> await(() -> fetchWhole(cmds, rawKey, type)));
        } catch (RuntimeException e) {
            return new Verdict(false, "could not read the sample from the source: " + rootMessage(e));
        }
        if (payload == null || expected.isEmpty()) {
            return new Verdict(false, "the sample key vanished before it could be probed");
        }

        String scratch = "r2dfly:restore-probe:" + UUID.randomUUID();
        byte[] rawScratch = scratch.getBytes(StandardCharsets.UTF_8);
        try {
            Optional<List<KeyRecord>> actual;
            try {
                target.withConnection(cmds -> await(() -> cmds.restore(rawScratch, payload, new RestoreArgs().replace()).get()));
            } catch (RuntimeException e) {
                return new Verdict(false, "target rejected the payload: " + rootMessage(e));
            }
            actual = target.withConnection(cmds -> await(() -> fetchWhole(cmds, rawScratch, type)));
            if (actual.isEmpty()) {
                return new Verdict(false, "RESTORE succeeded but the scratch key reads back empty");
            }
            JsonNode want = ComparableValue.of(expected.get().get(0));
            JsonNode got = ComparableValue.of(actual.get().get(0));
            return want.equals(got)
                    ? new Verdict(true, "restored value matches the source")
                    : new Verdict(false, "restored value differs from the source: expected " + want + " but read " + got);
        } catch (RuntimeException e) {
            return new Verdict(false, "probe failed on the target: " + rootMessage(e));
        } finally {
            try {
                target.withConnection(cmds -> await(() -> cmds.del(rawScratch).get()));
            } catch (RuntimeException ignored) {
                // a leftover scratch key is named for what it is and harmless to the migration
            }
        }
    }

    /** Chunk sizes of MAX_VALUE keep the whole value in one record so the two sides compare directly. */
    private static Optional<List<KeyRecord>> fetchWhole(
            io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands<byte[], byte[]> cmds, byte[] key, String type) throws Exception {
        return RedisValueFetcher.fetch(cmds, key, type, -1L, null, Integer.MAX_VALUE, Integer.MAX_VALUE, 1000, true);
    }

    private interface Call<T> {
        T run() throws Exception;
    }

    private static <T> T await(Call<T> call) {
        try {
            return call.run();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String rootMessage(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage();
    }
}
