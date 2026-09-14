package io.dragonfly.nifi.redis.util;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** JSON (de)serialization of {@link KeyRecord}, per spec section 4.2. */
public final class RedisTypeSerializer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private RedisTypeSerializer() {
    }

    public static byte[] toJsonBytes(KeyRecord record) throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(record);
    }

    public static void writeJson(KeyRecord record, OutputStream out) throws IOException {
        OBJECT_MAPPER.writeValue(out, record);
    }

    public static KeyRecord readJson(InputStream in) throws IOException {
        return OBJECT_MAPPER.readValue(in, KeyRecord.class);
    }

    public static KeyRecord readJson(byte[] bytes) throws IOException {
        return OBJECT_MAPPER.readValue(bytes, KeyRecord.class);
    }
}
