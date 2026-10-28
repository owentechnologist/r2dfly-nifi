package io.dragonfly.nifi.redis.util;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisAsyncCommandsImpl;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.RedisCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;

/**
 * Renders a {@link KeyRecord} as the RESP commands {@link CommandBuilder} sends to a target, by
 * running it against a connection that records each command instead of sending it. Every
 * command completes at once with an empty reply, except TIME, which answers this host's clock.
 * So stream delivery times come out absolute, while key and field TTLs stay relative and count
 * from when the file is loaded.
 */
public final class RespRecorder {

    private RespRecorder() {
    }

    public static byte[] encode(KeyRecord record) {
        ByteBuf out = Unpooled.buffer();
        StatefulRedisConnection<byte[], byte[]> connection = recordingConnection(out);
        CommandBuilder.write(new RedisAsyncCommandsImpl<>(connection, ByteArrayCodec.INSTANCE), connection, record,
                record.keyBytes(), CommandBuilder.TtlStrategy.PRESERVE, 0, CommandBuilder.ConflictStrategy.OVERWRITE).join();
        byte[] bytes = new byte[out.readableBytes()];
        out.readBytes(bytes);
        return bytes;
    }

    @SuppressWarnings("unchecked")
    private static StatefulRedisConnection<byte[], byte[]> recordingConnection(ByteBuf out) {
        return (StatefulRedisConnection<byte[], byte[]>) Proxy.newProxyInstance(RespRecorder.class.getClassLoader(),
                new Class<?>[] {StatefulRedisConnection.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "dispatch" -> {
                        if (args[0] instanceof Collection<?> commands) {
                            commands.forEach(c -> record((RedisCommand<byte[], byte[], ?>) c, out));
                        } else {
                            record((RedisCommand<byte[], byte[], ?>) args[0], out);
                        }
                        yield args[0];
                    }
                    case "getOptions" -> ClientOptions.create();
                    case "getTimeout" -> Duration.ofSeconds(60);
                    case "isOpen" -> true;
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    case "toString" -> "RespRecorder";
                    default -> null;
                });
    }

    private static void record(RedisCommand<byte[], byte[], ?> command, ByteBuf out) {
        if (command.getType().toString().equals(CommandType.TIME.name())) {
            long micros = System.currentTimeMillis() * 1000;
            command.getOutput().multi(2);
            command.getOutput().set(ascii(micros / 1_000_000));
            command.getOutput().set(ascii(micros % 1_000_000));
        } else {
            command.encode(out);
        }
        command.complete();
    }

    private static ByteBuffer ascii(long value) {
        return ByteBuffer.wrap(Long.toString(value).getBytes(StandardCharsets.US_ASCII));
    }
}
