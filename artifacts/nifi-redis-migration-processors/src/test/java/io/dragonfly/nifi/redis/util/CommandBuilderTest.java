package io.dragonfly.nifi.redis.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.PipelinedRedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.RedisCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * No mocking framework on this classpath (see {@code FakeRedisConnectionPoolService}) - both
 * command surfaces {@link CommandBuilder} needs are reflective proxies serving only the calls
 * it actually makes.
 */
class CommandBuilderTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void writesJsonValueViaRawJsonSetWithoutLeadingDel() throws Exception {
        FakeCommands fake = new FakeCommands();
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("name", "Owen");
        value.put("nested", Map.of("a", 1));
        KeyRecord record = new KeyRecord("doc:1", "json", 5000, null, value);

        CommandBuilder.write(fake.asyncCommands(), fake.rawConnection(), record, null, ":",
                CommandBuilder.TtlStrategy.PRESERVE, 0, CommandBuilder.ConflictStrategy.OVERWRITE).get();

        assertFalse(fake.delCalled, "JSON.SET already replaces the whole document; a leading DEL is redundant");
        String wire = fake.dispatchedCommandWireText();
        assertTrue(wire.contains("JSON.SET"), "expected JSON.SET on the wire, got: " + wire);
        assertTrue(wire.contains("doc:1"), "expected the key on the wire, got: " + wire);
        String expectedJson = new String(OBJECT_MAPPER.writeValueAsBytes(value), StandardCharsets.UTF_8);
        assertTrue(wire.contains(expectedJson), "expected serialized value on the wire, got: " + wire);
        assertEquals(List.of(5000L), fake.pexpireCalls, "TTL must be applied after JSON.SET");
    }

    @Test
    void hashStillRoundTripsThroughNewWriteSignature() throws Exception {
        FakeCommands fake = new FakeCommands();
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("field1", "value1");
        KeyRecord record = new KeyRecord("myhash", "hash", 0, null, value);

        CommandBuilder.write(fake.asyncCommands(), fake.rawConnection(), record, null, ":",
                CommandBuilder.TtlStrategy.PRESERVE, 0, CommandBuilder.ConflictStrategy.OVERWRITE).get();

        assertTrue(fake.delCalled, "hash writes still issue a leading DEL, unlike json");
        assertEquals(1, fake.hsetCalls.size());
        assertEquals(Map.of("field1", "value1"), decode(fake.hsetCalls.get(0)));
    }

    private static Map<String, String> decode(Map<byte[], byte[]> raw) {
        Map<String, String> decoded = new LinkedHashMap<>();
        raw.forEach((k, v) -> decoded.put(new String(k, StandardCharsets.UTF_8), new String(v, StandardCharsets.UTF_8)));
        return decoded;
    }

    /** Fakes the two connection views {@code AbstractRedisConnectionPoolService#withConnectionAndRaw}
     * hands to a caller from one borrowed connection - see {@link CommandBuilder#write}. */
    private static final class FakeCommands {
        boolean delCalled;
        final List<Long> pexpireCalls = new ArrayList<>();
        final List<Map<byte[], byte[]>> hsetCalls = new ArrayList<>();
        private Object dispatchedCommand;

        @SuppressWarnings("unchecked")
        RedisClusterAsyncCommands<byte[], byte[]> asyncCommands() {
            return (RedisClusterAsyncCommands<byte[], byte[]>) Proxy.newProxyInstance(
                    FakeCommands.class.getClassLoader(),
                    new Class<?>[]{RedisClusterAsyncCommands.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "del":
                                delCalled = true;
                                return completed(1L);
                            case "hset":
                                hsetCalls.add((Map<byte[], byte[]>) args[1]);
                                return completed(true);
                            case "pexpire":
                                pexpireCalls.add((Long) args[1]);
                                return completed(true);
                            case "exists":
                                return completed(0L);
                            default:
                                throw new UnsupportedOperationException(method.getName());
                        }
                    });
        }

        @SuppressWarnings("unchecked")
        StatefulConnection<byte[], byte[]> rawConnection() {
            return (StatefulConnection<byte[], byte[]>) Proxy.newProxyInstance(
                    FakeCommands.class.getClassLoader(),
                    new Class<?>[]{StatefulConnection.class},
                    (proxy, method, args) -> {
                        if ("dispatch".equals(method.getName()) && args.length == 1 && args[0] instanceof RedisCommand) {
                            dispatchedCommand = args[0];
                            // AsyncCommand extends CompletableFuture<T> itself; completing it directly
                            // stands in for the reply a real connection would deliver off the wire.
                            ((CompletableFuture<Object>) dispatchedCommand).complete("OK");
                            return dispatchedCommand;
                        }
                        throw new UnsupportedOperationException(method.getName());
                    });
        }

        String dispatchedCommandWireText() {
            ByteBuf buf = Unpooled.buffer();
            ((AsyncCommand<?, ?, ?>) dispatchedCommand).encode(buf);
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private static <T> RedisFuture<T> completed(T value) {
            return new PipelinedRedisFuture<>(CompletableFuture.completedFuture(value));
        }
    }
}
