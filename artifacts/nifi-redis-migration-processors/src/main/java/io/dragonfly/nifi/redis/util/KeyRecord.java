package io.dragonfly.nifi.redis.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON envelope written as FlowFile content by {@code RedisTypeDeserializer}/
 * {@code RedisSingleKeyFetch} and read back by {@code RedisBatchWriter}, per spec section 4.2.
 * {@code value}'s shape depends on {@code type}: a String for {@code string}, a
 * {@code Map<String,String>} for {@code hash}, a {@code List<String>} for {@code list}/
 * {@code set}, a {@code List<Map<String,Object>>} of {@code {member,score}} for {@code zset},
 * and a {@code List<Map<String,Object>>} of {@code {id,fields}} for {@code stream}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KeyRecord {

    public String key;
    public String type;

    @JsonProperty("ttl_ms")
    public long ttlMs;

    public String encoding;
    public Object value;

    @JsonProperty("consumer_groups")
    public Object consumerGroups;

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
}
