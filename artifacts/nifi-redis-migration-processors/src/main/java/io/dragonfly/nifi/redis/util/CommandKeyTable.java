package io.dragonfly.nifi.redis.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Which arguments of a command are keys, taken from the source's own {@code COMMAND} reply so it
 * covers that server's exact command set, modules included (JSON.SET, TOPK.ADD and the like).
 * Commands whose key positions depend on their arguments (flag {@code movablekeys}), and commands
 * the table does not know, are resolved with {@code COMMAND GETKEYS} on the source.
 */
public final class CommandKeyTable {

    private record Spec(int first, int last, int step, boolean movable, boolean hasSubcommands) {
    }

    private final Map<String, Spec> specs;
    private final Function<List<byte[]>, List<byte[]>> getKeys;

    private CommandKeyTable(Map<String, Spec> specs, Function<List<byte[]>, List<byte[]>> getKeys) {
        this.specs = specs;
        this.getKeys = getKeys;
    }

    /** {@code commandReply} is the raw {@code COMMAND} reply; {@code getKeys} answers {@code COMMAND GETKEYS <argv>}. */
    public static CommandKeyTable fromCommandReply(List<Object> commandReply, Function<List<byte[]>, List<byte[]>> getKeys) {
        Map<String, Spec> specs = new HashMap<>();
        for (Object entry : commandReply) {
            addSpec(specs, (List<?>) entry);
        }
        return new CommandKeyTable(specs, getKeys);
    }

    private static void addSpec(Map<String, Spec> specs, List<?> entry) {
        String name = text(entry.get(0)).toLowerCase();
        List<?> flags = (List<?>) entry.get(2);
        boolean movable = flags.stream().anyMatch(f -> text(f).equalsIgnoreCase("movablekeys"));
        boolean hasSubcommands = entry.size() > 9 && entry.get(9) instanceof List<?> subs && !subs.isEmpty();
        specs.put(name, new Spec(number(entry.get(3)), number(entry.get(4)), number(entry.get(5)), movable, hasSubcommands));
        if (hasSubcommands) {
            for (Object sub : (List<?>) entry.get(9)) {
                addSpec(specs, (List<?>) sub);
            }
        }
    }

    /** The keys {@code argv} touches. Empty for keyless commands such as PUBLISH or FUNCTION LOAD. */
    public List<byte[]> keys(List<byte[]> argv) {
        String name = new String(argv.get(0), StandardCharsets.UTF_8).toLowerCase();
        Spec spec = specs.get(name);
        if (spec != null && spec.hasSubcommands() && argv.size() > 1) {
            Spec sub = specs.get(name + "|" + new String(argv.get(1), StandardCharsets.UTF_8).toLowerCase());
            spec = sub != null ? sub : spec;
        }
        if (spec == null || spec.movable()) {
            return getKeys.apply(argv);
        }
        if (spec.first() <= 0) {
            return List.of();
        }
        int last = spec.last() < 0 ? argv.size() + spec.last() : spec.last();
        int step = Math.max(spec.step(), 1);
        List<byte[]> keys = new ArrayList<>();
        for (int i = spec.first(); i <= last && i < argv.size(); i += step) {
            keys.add(argv.get(i));
        }
        return keys;
    }

    private static String text(Object value) {
        return value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(value);
    }

    private static int number(Object value) {
        return value instanceof Number n ? n.intValue() : Integer.parseInt(text(value));
    }
}
