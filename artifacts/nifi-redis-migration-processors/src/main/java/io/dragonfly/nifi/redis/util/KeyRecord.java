package io.dragonfly.nifi.redis.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * JSON envelope written as FlowFile content by {@code RedisTypeDeserializer}/
 * {@code RedisSingleKeyFetch} and read back by {@code RedisBatchWriter}, per spec section 4.2.
 * {@code value}'s shape depends on {@code type}: a String for {@code string}, a
 * {@code Map<String,String>} for {@code hash}, a {@code List<String>} for {@code list}/
 * {@code set}, a {@code List<Map<String,Object>>} of {@code {member,score}} for {@code zset},
 * a {@code List<Map<String,Object>>} of {@code {id,fields}} for {@code stream}, and - for a
 * key written as a native Dragonfly JSON document rather than sourced from Redis - {@code json},
 * in which case {@code value} is the parsed JSON document itself (whatever Jackson's untyped
 * {@code Object} binding produces: {@code Map<String,Object>}/{@code List<Object>}/
 * {@code String}/{@code Number}/{@code Boolean}/{@code null}), not a pre-encoded JSON string.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KeyRecord {

    /** Type of a record that deletes its key: the key was gone when it was read. */
    public static final String DELETED = "deleted";

    /** True when the key name is not valid UTF-8; {@code key} is then its bytes in base64. Absent otherwise. */
    @JsonProperty("key_base64")
    public Boolean keyBase64;

    public String key;
    public String type;

    @JsonProperty("ttl_ms")
    public long ttlMs;

    public String encoding;
    public Object value;

    /**
     * True when any byte string in the value is not valid UTF-8. Then every one of them (a string,
     * hash fields and values, list, set and zset members, stream field names and values) is base64,
     * so they survive JSON exactly. Absent for a value that is all text.
     */
    public Boolean base64;

    @JsonProperty("consumer_groups")
    public Object consumerGroups;

    /** Stream only: the source's last-generated-id, which can be ahead of the last remaining entry. */
    @JsonProperty("last_generated_id")
    public String lastGeneratedId;

    /** Hash only: field to its expiry deadline in epoch ms on the reading host's clock. */
    @JsonProperty("field_expire_at_ms")
    public java.util.Map<String, Long> fieldExpireAtMs;

    /** Raw DUMP payload for a --dfly-to-dfly write - when set, {@link CommandBuilder#write}
     * issues a single RESTORE instead of a type-specific write, bypassing {@code value}
     * entirely. Set directly by {@code RedisBatchWriter} from a FlowFile's raw content when it
     * carries the {@code redis.dfly-dump} attribute; {@code @JsonIgnore} enforces that it never
     * round-trips through the JSON envelope {@link RedisTypeSerializer} uses for the normal path. */
    @JsonIgnore
    public byte[] dumpPayload;

    public KeyRecord() {
    }

    public KeyRecord(String key, String type, long ttlMs, String encoding, Object value) {
        this.key = key;
        this.type = type;
        this.ttlMs = ttlMs;
        this.encoding = encoding;
        this.value = value;
    }

    /** A record for {@code key} whose value's byte strings were written with {@link #text(byte[], boolean)} under {@code base64}. */
    public static KeyRecord of(byte[] key, boolean base64, String type, long ttlMs, String encoding, Object value) {
        boolean keyBase64 = !RedisBytes.isText(key);
        KeyRecord record = new KeyRecord(text(key, keyBase64), type, ttlMs, encoding, value);
        record.keyBase64 = keyBase64 ? Boolean.TRUE : null;
        record.base64 = base64 ? Boolean.TRUE : null;
        return record;
    }

    /** Whether a value holding these byte strings has to be base64. */
    public static boolean needsBase64(Iterable<byte[]> values) {
        for (byte[] value : values) {
            if (value != null && !RedisBytes.isText(value)) {
                return true;
            }
        }
        return false;
    }

    public static String text(byte[] bytes, boolean base64) {
        return base64 ? Base64.getEncoder().encodeToString(bytes) : new String(bytes, StandardCharsets.UTF_8);
    }


    public byte[] keyBytes() {
        return Boolean.TRUE.equals(keyBase64) ? Base64.getDecoder().decode(key) : key.getBytes(StandardCharsets.UTF_8);
    }

    /** One of this record's byte strings, from its text form. */
    public byte[] bytes(Object text) {
        String value = String.valueOf(text);
        return Boolean.TRUE.equals(base64) ? Base64.getDecoder().decode(value) : value.getBytes(StandardCharsets.UTF_8);
    }
}
