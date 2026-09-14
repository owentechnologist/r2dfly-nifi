package io.dragonfly.nifi.redis.util;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A normalized, JSON-(de)serializable snapshot of one {@code FT.INFO} reply, plus the logic to
 * turn it back into an {@code FT.CREATE} argument list. Used by {@code SearchIndexExporter}
 * (parses the source's {@code FT.INFO} reply, caches the result) and {@code
 * SearchIndexRehydrator} (reads the cached definition back, rebuilds the index on the target).
 *
 * <p>Ported from this project's original {@code lua/search-index-export.lua} (parsing) and
 * {@code rehydrate-search-indexes.sh}'s Python {@code json_to_ft_create_args} (arg-building), now
 * done directly in Java instead of shelling out - {@code FT._LIST}/{@code FT.INFO}/{@code
 * FT.CREATE} are all single, statically-keyed (non-key) commands, so none of this ever needed
 * Lua's undeclared-key workaround in the first place; that was purely a side effect of the old
 * design's ordinary-string-pipeline smuggling.
 *
 * <p>{@code FT.INFO}'s per-attribute reply is a flat list, uneven length (boolean flags like
 * SORTABLE appear as a lone trailing element, no paired value), so fields are read by scanning
 * for a known key/flag rather than pairing the whole list - and that scan has to continue from
 * where the previous one left off, not always restart from position 0: a field whose own alias
 * happens to literally be the string "type" (e.g. identifier "$.type", attribute "type") puts
 * "type" in the list twice - once as that alias's value, once as the real key marker for the
 * schema type right after it - and searching for "type" from the start would match the alias's
 * value first, returning the wrong thing entirely (found live in the original Lua version: an
 * index with a JSON field named "type" exported as {@code {"type": "type"}} instead of the real
 * schema type). identifier/attribute/type (and, for VECTOR, algorithm/data_type/dim/
 * distance_metric/initial_cap/M/ef_construction/ef_runtime/epsilon) are always emitted in that
 * fixed order per FT.INFO's own convention, so each lookup below starts searching right after the
 * previous one ended.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SearchIndexDefinition {

    public String index;
    public String keyType = "HASH";
    public List<String> prefixes = new ArrayList<>();
    public boolean indexesAll;
    public String filter;
    public String language;
    public String score;
    public List<Field> attributes = new ArrayList<>();

    public SearchIndexDefinition() {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Field {
        public String identifier;
        public String attribute;
        public String type;
        public String weight;
        public String separator;
        public boolean sortable;
        public boolean unf;
        public boolean nostem;
        public boolean casesensitive;
        public boolean noindex;
        public String algorithm;
        public String dataType;
        public String dim;
        public String distanceMetric;
        public String initialCap;
        public String m;
        public String efConstruction;
        public String efRuntime;
        public String epsilon;

        public Field() {
        }
    }

    /** Tracks a scan's result (the value found, or null) alongside the index the *next* scan
     * should start from - mirrors search-index-export.lua's findValueFrom two-return-value shape. */
    private record Found(String value, int nextIndex) {
    }

    /** Decodes one {@code FT.INFO} reply element - a {@code byte[]} for bulk/simple strings (via
     * {@link RawModuleCommands#toStringValue}), or anything else ({@code Long}/{@code Double},
     * already-a-{@code List} for a nested sub-array) via {@code toString()}/direct cast. */
    private static String str(Object o) {
        return o == null ? null : RawModuleCommands.toStringValue(o);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object o) {
        return (o instanceof List) ? (List<Object>) o : null;
    }

    private static Object findValue(List<Object> list, String key) {
        for (int i = 0; i < list.size(); i++) {
            if (key.equals(str(list.get(i)))) {
                return i + 1 < list.size() ? list.get(i + 1) : null;
            }
        }
        return null;
    }

    private static Found findFrom(List<Object> list, String key, int fromIndex) {
        for (int i = fromIndex; i < list.size(); i++) {
            if (key.equals(str(list.get(i)))) {
                String value = i + 1 < list.size() ? str(list.get(i + 1)) : null;
                return new Found(value, i + 2);
            }
        }
        return new Found(null, fromIndex);
    }

    private static boolean hasFlag(List<Object> list, String flag, int fromIndex) {
        for (int i = fromIndex; i < list.size(); i++) {
            if (flag.equals(str(list.get(i)))) {
                return true;
            }
        }
        return false;
    }

    /** Parses one {@code FT.INFO <indexName>} reply (as decoded by {@link
     * RawModuleCommands#ftInfo}) into a normalized definition. {@code indexName} comes from the
     * {@code FT._LIST} entry that produced this call, not from the reply itself (FT.INFO does
     * report its own "index_name" field too, but there's no reason to trust it over the name we
     * already know we asked for). */
    public static SearchIndexDefinition parse(String indexName, List<Object> info) {
        SearchIndexDefinition def = new SearchIndexDefinition();
        def.index = indexName;

        List<Object> rawDef = asList(findValue(info, "index_definition"));
        if (rawDef != null) {
            String keyType = str(findValue(rawDef, "key_type"));
            if (keyType != null) {
                def.keyType = keyType;
            }
            List<Object> rawPrefixes = asList(findValue(rawDef, "prefixes"));
            if (rawPrefixes != null) {
                for (Object p : rawPrefixes) {
                    def.prefixes.add(str(p));
                }
            }
            Object indexesAll = findValue(rawDef, "indexes_all");
            def.indexesAll = "true".equals(str(indexesAll)) || Boolean.TRUE.equals(indexesAll);
            def.filter = str(findValue(rawDef, "filter"));
            def.language = str(findValue(rawDef, "language"));
            def.score = str(findValue(rawDef, "default_score"));
        }

        List<Object> rawAttrs = asList(findValue(info, "attributes"));
        if (rawAttrs != null) {
            for (Object rawAttrObj : rawAttrs) {
                List<Object> rawAttr = asList(rawAttrObj);
                if (rawAttr != null) {
                    def.attributes.add(parseField(rawAttr));
                }
            }
        }
        return def;
    }

    private static Field parseField(List<Object> rawAttr) {
        Field f = new Field();
        Found identifier = findFrom(rawAttr, "identifier", 0);
        Found attribute = findFrom(rawAttr, "attribute", identifier.nextIndex());
        Found type = findFrom(rawAttr, "type", attribute.nextIndex());
        Found weight = findFrom(rawAttr, "WEIGHT", type.nextIndex());
        Found separator = findFrom(rawAttr, "SEPARATOR", type.nextIndex());
        // VECTOR fields carry their own plain key/value run right after "type" (FT.INFO order,
        // verified directly against a live Dragonfly instance: algorithm, data_type, dim,
        // distance_metric, initial_cap, then M/ef_construction/ef_runtime/epsilon for HNSW only)
        // instead of the WEIGHT/SEPARATOR/flag shape TEXT/TAG use - each lookup still chains from
        // the previous one's end position for the same reason findFrom does everywhere else.
        Found algorithm = findFrom(rawAttr, "algorithm", type.nextIndex());
        Found dataType = findFrom(rawAttr, "data_type", algorithm.nextIndex());
        Found dim = findFrom(rawAttr, "dim", dataType.nextIndex());
        Found distanceMetric = findFrom(rawAttr, "distance_metric", dim.nextIndex());
        Found initialCap = findFrom(rawAttr, "initial_cap", distanceMetric.nextIndex());
        Found m = findFrom(rawAttr, "M", initialCap.nextIndex());
        Found efConstruction = findFrom(rawAttr, "ef_construction", m.nextIndex());
        Found efRuntime = findFrom(rawAttr, "ef_runtime", efConstruction.nextIndex());
        Found epsilon = findFrom(rawAttr, "epsilon", efRuntime.nextIndex());

        f.identifier = identifier.value();
        f.attribute = attribute.value();
        f.type = type.value();
        f.weight = weight.value();
        f.separator = separator.value();
        f.sortable = hasFlag(rawAttr, "SORTABLE", type.nextIndex());
        f.unf = hasFlag(rawAttr, "UNF", type.nextIndex());
        f.nostem = hasFlag(rawAttr, "NOSTEM", type.nextIndex());
        f.casesensitive = hasFlag(rawAttr, "CASESENSITIVE", type.nextIndex());
        f.noindex = hasFlag(rawAttr, "NOINDEX", type.nextIndex());
        f.algorithm = algorithm.value();
        f.dataType = dataType.value();
        f.dim = dim.value();
        f.distanceMetric = distanceMetric.value();
        f.initialCap = initialCap.value();
        f.m = m.value();
        f.efConstruction = efConstruction.value();
        f.efRuntime = efRuntime.value();
        f.epsilon = epsilon.value();
        return f;
    }

    /**
     * Builds the {@code FT.CREATE} argument list (everything after the command name itself) this
     * definition needs to be reconstructed on a target. Throws {@link
     * UnsupportedSearchIndexException} if a VECTOR field can't be reconstructed - callers should
     * catch this per-index and skip just that index, not abort the whole rehydration run.
     */
    public List<byte[]> toFtCreateArgs() {
        List<byte[]> args = new ArrayList<>();
        args.add(bytes(index));
        args.add(bytes("ON"));
        args.add(bytes(keyType != null ? keyType : "HASH"));

        if (prefixes != null && !prefixes.isEmpty() && !indexesAll) {
            args.add(bytes("PREFIX"));
            args.add(bytes(String.valueOf(prefixes.size())));
            for (String p : prefixes) {
                args.add(bytes(p));
            }
        }
        if (!isBlank(filter)) {
            args.add(bytes("FILTER"));
            args.add(bytes(filter));
        }
        if (!isBlank(language)) {
            args.add(bytes("LANGUAGE"));
            args.add(bytes(language));
        }
        if (!isBlank(score)) {
            args.add(bytes("SCORE"));
            args.add(bytes(score));
        }

        args.add(bytes("SCHEMA"));
        int schemaIndex = args.size() - 1;

        if (attributes != null) {
            for (Field attr : attributes) {
                appendField(args, attr);
            }
        }

        if (args.size() - 1 == schemaIndex) {
            throw new UnsupportedSearchIndexException("index '" + index + "': no reconstructable fields");
        }
        return args;
    }

    private void appendField(List<byte[]> args, Field attr) {
        String ftype = attr.type == null ? "" : attr.type.toUpperCase(Locale.ROOT);
        if (ftype.isEmpty() || attr.identifier == null) {
            return;
        }

        args.add(bytes(attr.identifier));
        if (attr.attribute != null && !attr.attribute.equals(attr.identifier)) {
            args.add(bytes("AS"));
            args.add(bytes(attr.attribute));
        }
        args.add(bytes(ftype));

        switch (ftype) {
            case "TEXT" -> {
                if (!isBlank(attr.weight) && !"1".equals(attr.weight)) {
                    args.add(bytes("WEIGHT"));
                    args.add(bytes(attr.weight));
                }
                if (attr.nostem) {
                    args.add(bytes("NOSTEM"));
                }
                if (attr.sortable) {
                    args.add(bytes("SORTABLE"));
                    if (attr.unf) {
                        args.add(bytes("UNF"));
                    }
                }
                if (attr.noindex) {
                    args.add(bytes("NOINDEX"));
                }
            }
            case "TAG" -> {
                if (!isBlank(attr.separator)) {
                    args.add(bytes("SEPARATOR"));
                    args.add(bytes(attr.separator));
                }
                if (attr.casesensitive) {
                    args.add(bytes("CASESENSITIVE"));
                }
                if (attr.sortable) {
                    args.add(bytes("SORTABLE"));
                    if (attr.unf) {
                        args.add(bytes("UNF"));
                    }
                }
                if (attr.noindex) {
                    args.add(bytes("NOINDEX"));
                }
            }
            case "NUMERIC", "GEO" -> {
                if (attr.sortable) {
                    args.add(bytes("SORTABLE"));
                }
                if (attr.noindex) {
                    args.add(bytes("NOINDEX"));
                }
            }
            case "VECTOR" -> appendVectorField(args, attr);
            default -> {
                // Unrecognized schema type - leave it as just "<identifier> [AS <attribute>]
                // <TYPE>" with no extra args, matching the original script's behavior of never
                // rejecting a type it doesn't specifically know how to decorate.
            }
        }
    }

    private void appendVectorField(List<byte[]> args, Field attr) {
        String algorithm = attr.algorithm == null ? "" : attr.algorithm.toUpperCase(Locale.ROOT);
        if (!("FLAT".equals(algorithm) || "HNSW".equals(algorithm))
                || isBlank(attr.dataType) || isBlank(attr.dim) || isBlank(attr.distanceMetric)) {
            throw new UnsupportedSearchIndexException(
                    "index '" + index + "' field '" + attr.identifier + "' is VECTOR but FT.INFO didn't report a "
                            + "reconstructable algorithm/data_type/dim/distance_metric (algorithm=" + attr.algorithm
                            + ", data_type=" + attr.dataType + ", dim=" + attr.dim
                            + ", distance_metric=" + attr.distanceMetric + ") - vector similarity index migration "
                            + "isn't supported for this field; skipping this index entirely (not migrated)");
        }
        List<byte[]> vectorArgs = new ArrayList<>();
        vectorArgs.add(bytes("TYPE"));
        vectorArgs.add(bytes(attr.dataType.toUpperCase(Locale.ROOT)));
        vectorArgs.add(bytes("DIM"));
        vectorArgs.add(bytes(attr.dim));
        vectorArgs.add(bytes("DISTANCE_METRIC"));
        vectorArgs.add(bytes(attr.distanceMetric.toUpperCase(Locale.ROOT)));
        if (!isBlank(attr.initialCap)) {
            vectorArgs.add(bytes("INITIAL_CAP"));
            vectorArgs.add(bytes(attr.initialCap));
        }
        if ("HNSW".equals(algorithm)) {
            addIfPresent(vectorArgs, "M", attr.m);
            addIfPresent(vectorArgs, "EF_CONSTRUCTION", attr.efConstruction);
            addIfPresent(vectorArgs, "EF_RUNTIME", attr.efRuntime);
            addIfPresent(vectorArgs, "EPSILON", attr.epsilon);
        }
        args.add(bytes(algorithm));
        args.add(bytes(String.valueOf(vectorArgs.size())));
        args.addAll(vectorArgs);
    }

    private static void addIfPresent(List<byte[]> args, String name, String value) {
        if (!isBlank(value)) {
            args.add(bytes(name));
            args.add(bytes(value));
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isEmpty();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
