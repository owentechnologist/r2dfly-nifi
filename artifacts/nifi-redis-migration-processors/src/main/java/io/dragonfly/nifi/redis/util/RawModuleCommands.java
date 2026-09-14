package io.dragonfly.nifi.redis.util;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueListOutput;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Dispatches Redis module commands (RedisJSON's {@code JSON.*}, RedisBloom's {@code TOPK.*})
 * that Lettuce's typed command interfaces don't cover, via its low-level raw dispatch API. Used
 * by {@code ModuleTypeHandler} to read/write the two module types spec section 8.3 names as
 * examples (ReJSON-RL, TopK); other module types fall through to the dead-letter queue.
 */
public final class RawModuleCommands {

    private enum ModuleCommand implements ProtocolKeyword {
        JSON_GET("JSON.GET"), JSON_SET("JSON.SET"),
        TOPK_RESERVE("TOPK.RESERVE"), TOPK_ADD("TOPK.ADD"), TOPK_INCRBY("TOPK.INCRBY"),
        TOPK_LIST("TOPK.LIST"), TOPK_INFO("TOPK.INFO"),
        FT_LIST("FT._LIST"), FT_INFO("FT.INFO"), FT_CREATE("FT.CREATE");

        private final byte[] bytes;

        ModuleCommand(String name) {
            this.bytes = name.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }

    private RawModuleCommands() {
    }

    public static CompletableFuture<byte[]> jsonGet(StatefulConnection<byte[], byte[]> conn, byte[] key) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addKey(key);
        return dispatch(conn, ModuleCommand.JSON_GET, new io.lettuce.core.output.ByteArrayOutput<>(ByteArrayCodec.INSTANCE), args);
    }

    public static CompletableFuture<String> jsonSet(StatefulConnection<byte[], byte[]> conn, byte[] key, byte[] rawJson) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addKey(key).add(".").addValue(rawJson);
        return dispatch(conn, ModuleCommand.JSON_SET, new StatusOutput<>(ByteArrayCodec.INSTANCE), args);
    }

    /**
     * Flat {name: value} params from {@code TOPK.INFO} (k, width, depth, decay). Uses
     * {@link ArrayOutput} rather than {@link ValueListOutput} because the reply mixes RESP
     * integer elements (k, width, depth) with a bulk-string element (decay) -
     * {@code ValueListOutput} only decodes bulk-string elements and throws
     * {@code UnsupportedOperationException} the moment it hits an integer one.
     */
    public static CompletableFuture<Map<String, String>> topkInfo(StatefulConnection<byte[], byte[]> conn, byte[] key) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addKey(key);
        return dispatch(conn, ModuleCommand.TOPK_INFO, new ArrayOutput<>(ByteArrayCodec.INSTANCE), args)
                .thenApply(RawModuleCommands::toFlatMap);
    }

    /**
     * {member, count} pairs from {@code TOPK.LIST key WITHCOUNT}. Same integer/bulk-string
     * mixed-reply issue as {@link #topkInfo} - counts come back as RESP integers.
     */
    public static CompletableFuture<List<Map.Entry<String, Long>>> topkListWithCount(StatefulConnection<byte[], byte[]> conn, byte[] key) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addKey(key).add("WITHCOUNT");
        return dispatch(conn, ModuleCommand.TOPK_LIST, new ArrayOutput<>(ByteArrayCodec.INSTANCE), args)
                .thenApply(list -> {
                    List<Map.Entry<String, Long>> result = new ArrayList<>();
                    for (int i = 0; i + 1 < list.size(); i += 2) {
                        result.add(Map.entry(toStringValue(list.get(i)), toLongValue(list.get(i + 1))));
                    }
                    return result;
                });
    }

    public static CompletableFuture<String> topkReserve(StatefulConnection<byte[], byte[]> conn, byte[] key, long k, long width, long depth, double decay) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE)
                .addKey(key).add(k).add(width).add(depth).add(decay);
        return dispatch(conn, ModuleCommand.TOPK_RESERVE, new StatusOutput<>(ByteArrayCodec.INSTANCE), args);
    }

    public static CompletableFuture<List<byte[]>> topkAdd(StatefulConnection<byte[], byte[]> conn, byte[] key, List<byte[]> items) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addKey(key).addValues(items);
        return dispatch(conn, ModuleCommand.TOPK_ADD, new ValueListOutput<>(ByteArrayCodec.INSTANCE), args);
    }

    /**
     * Restores exact per-item counts via {@code TOPK.INCRBY key item increment [item increment
     * ...]} - unlike {@link #topkAdd} (each call only registers membership, incrementing by 1),
     * this reproduces the source's real relative frequencies. Callers must pre-chunk {@code
     * pairs} themselves: RedisBloom/Dragonfly reject a single increment over 100000 (found live
     * against production-scale data), so any
     * one item whose full count exceeds that has to be split across multiple increments for the
     * same item before calling this - this method just dispatches whichever pairs it's given as
     * one command.
     */
    public static CompletableFuture<String> topkIncrBy(StatefulConnection<byte[], byte[]> conn, byte[] key, List<Map.Entry<byte[], Long>> pairs) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addKey(key);
        for (Map.Entry<byte[], Long> pair : pairs) {
            args.addValue(pair.getKey()).add(pair.getValue());
        }
        return dispatch(conn, ModuleCommand.TOPK_INCRBY, new StatusOutput<>(ByteArrayCodec.INSTANCE), args);
    }

    /** Index names from {@code FT._LIST} - a flat array of bulk strings only, so {@link
     * ValueListOutput} (unlike {@link #topkInfo}/{@link #topkListWithCount}) is sufficient. */
    public static CompletableFuture<List<byte[]>> ftList(StatefulConnection<byte[], byte[]> conn) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE);
        return dispatch(conn, ModuleCommand.FT_LIST, new ValueListOutput<>(ByteArrayCodec.INSTANCE), args);
    }

    /**
     * The full {@code FT.INFO <indexName>} reply, decoded via {@link NestedMultiOutput} rather
     * than {@link ArrayOutput} because - unlike every other raw reply this class decodes - it's
     * genuinely nested: top-level elements like "attributes" are themselves sub-arrays (one per
     * schema field), not just a mix of scalar types. {@link SearchIndexDefinition#parse} does the
     * actual field-by-field interpretation of the result.
     */
    public static CompletableFuture<List<Object>> ftInfo(StatefulConnection<byte[], byte[]> conn, byte[] indexName) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addValue(indexName);
        return dispatch(conn, ModuleCommand.FT_INFO, new NestedMultiOutput<>(ByteArrayCodec.INSTANCE), args);
    }

    /** Issues {@code FT.CREATE <args...>} exactly as built by {@link
     * SearchIndexDefinition#toFtCreateArgs()} - generic since the argument shape depends entirely
     * on the index's own schema, unlike every other command here which has a fixed argument
     * layout. */
    public static CompletableFuture<String> ftCreate(StatefulConnection<byte[], byte[]> conn, List<byte[]> ftCreateArgs) {
        CommandArgs<byte[], byte[]> args = new CommandArgs<>(ByteArrayCodec.INSTANCE).addValues(ftCreateArgs);
        return dispatch(conn, ModuleCommand.FT_CREATE, new StatusOutput<>(ByteArrayCodec.INSTANCE), args);
    }

    private static Map<String, String> toFlatMap(List<Object> flat) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < flat.size(); i += 2) {
            map.put(toStringValue(flat.get(i)), toStringValue(flat.get(i + 1)));
        }
        return map;
    }

    /** Decodes an {@link ArrayOutput}/{@link NestedMultiOutput} element - a {@code byte[]} for
     * bulk strings, or a boxed {@code Long}/{@code Double} for RESP integer/double replies. Public
     * because {@link SearchIndexDefinition} also needs it for {@code FT.INFO}'s reply. */
    public static String toStringValue(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(value);
    }

    private static long toLongValue(Object value) {
        if (value instanceof Long l) {
            return l;
        }
        return Long.parseLong(toStringValue(value));
    }

    private static <T> CompletableFuture<T> dispatch(
            StatefulConnection<byte[], byte[]> conn, ProtocolKeyword type, CommandOutput<byte[], byte[], T> output, CommandArgs<byte[], byte[]> args) {
        Command<byte[], byte[], T> command = new Command<>(type, output, args);
        AsyncCommand<byte[], byte[], T> asyncCommand = new AsyncCommand<>(command);
        conn.dispatch(asyncCommand);
        return asyncCommand;
    }
}
