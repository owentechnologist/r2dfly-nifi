package io.dragonfly.nifi.redis.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A key's value in a form two reads of the same data compare equal in, whichever server they came
 * from: set members sorted (SMEMBERS has no order), and times that move between reads removed.
 * Key, TTL and encoding are left out; a hash's field-TTL deadlines too, being host-clock times.
 */
public final class ComparableValue {

    /** Idle and activity times move between two reads, and delivered_at_ms is derived from the
     * moment of each read, so none of them ever match exactly. A group's lag comes from the
     * stream's entries-added, which Dragonfly cannot be told, so it differs once entries were trimmed. */
    private static final Set<String> TIME_DEPENDENT_FIELDS = Set.of(
            "ms_since_last_delivery", "delivered_at_ms", "idle", "inactive", "seen-time", "active-time", "lag");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ComparableValue() {
    }

    public static JsonNode of(KeyRecord record) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", record.type);
        node.set("value", MAPPER.valueToTree(record.value));
        node.set("consumer_groups", MAPPER.valueToTree(record.consumerGroups));
        if ("set".equals(record.type) && node.get("value") instanceof ArrayNode members) {
            List<String> sorted = new ArrayList<>();
            members.forEach(m -> sorted.add(m.asText()));
            sorted.sort(null);
            node.set("value", MAPPER.valueToTree(sorted));
        }
        stripTimeDependentFields(node);
        return node;
    }

    private static void stripTimeDependentFields(JsonNode node) {
        if (node instanceof ObjectNode object) {
            object.remove(TIME_DEPENDENT_FIELDS);
            object.forEach(ComparableValue::stripTimeDependentFields);
        } else if (node instanceof ArrayNode array) {
            array.forEach(ComparableValue::stripTimeDependentFields);
        }
    }
}
