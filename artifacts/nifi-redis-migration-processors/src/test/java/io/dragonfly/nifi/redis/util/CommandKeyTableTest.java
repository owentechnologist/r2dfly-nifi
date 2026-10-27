package io.dragonfly.nifi.redis.util;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CommandKeyTableTest {

    /** Entries shaped like Redis 7's COMMAND reply: name, arity, flags, first, last, step, ..., subcommands at index 9. */
    private static final List<Object> COMMAND_REPLY = List.of(
            entry("set", -3, List.of("write"), 1, 1, 1, List.of()),
            entry("mset", -3, List.of("write"), 1, -1, 2, List.of()),
            entry("publish", 3, List.of("pubsub"), 0, 0, 0, List.of()),
            entry("zunionstore", -4, List.of("write", "movablekeys"), 1, 1, 1, List.of()),
            entry("xgroup", -2, List.of(), 0, 0, 0, List.of(
                    entry("xgroup|setid", -5, List.of("write"), 2, 2, 1, List.of()),
                    entry("xgroup|createconsumer", 5, List.of("write"), 2, 2, 1, List.of()))));

    @Test
    void readsKeyPositionsFromTheServersOwnCommandTable() {
        List<List<byte[]>> asked = new ArrayList<>();
        CommandKeyTable table = CommandKeyTable.fromCommandReply(COMMAND_REPLY, argv -> {
            asked.add(argv);
            return List.of(bytes("dest"), bytes("src1"));
        });

        assertEquals(List.of("k"), text(table.keys(argv("SET", "k", "v"))));
        assertEquals(List.of("a", "b", "c"), text(table.keys(argv("MSET", "a", "1", "b", "2", "c", "3"))));
        assertEquals(List.of("orders"), text(table.keys(argv("XGROUP", "SETID", "orders", "g1", "5-0"))));
        assertEquals(List.of(), text(table.keys(argv("PUBLISH", "channel", "message"))));
        assertEquals(List.of(), asked, "fixed key positions need no round trip");

        assertEquals(List.of("dest", "src1"), text(table.keys(argv("ZUNIONSTORE", "dest", "1", "src1"))));
        assertEquals(List.of("dest", "src1"), text(table.keys(argv("MODULE.UNKNOWN", "x"))));
        assertEquals(2, asked.size(), "movable keys and unknown commands go to COMMAND GETKEYS");
    }

    private static List<Object> entry(String name, int arity, List<String> flags, int first, int last, int step, List<Object> subcommands) {
        return List.of(bytes(name), (long) arity, flags.stream().map(CommandKeyTableTest::bytes).toList(),
                (long) first, (long) last, (long) step, List.of(), List.of(), List.of(), subcommands);
    }

    private static List<byte[]> argv(String... parts) {
        return java.util.Arrays.stream(parts).map(CommandKeyTableTest::bytes).toList();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> text(List<byte[]> keys) {
        return keys.stream().map(k -> new String(k, StandardCharsets.UTF_8)).toList();
    }
}
