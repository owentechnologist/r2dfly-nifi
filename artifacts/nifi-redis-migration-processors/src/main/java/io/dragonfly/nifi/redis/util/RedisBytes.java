package io.dragonfly.nifi.redis.util;

import org.apache.nifi.flowfile.FlowFile;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Redis keys and values are bytes, and NiFi attributes and JSON are text. A key name travels in
 * {@code redis.key} as UTF-8 text when it is valid UTF-8; otherwise {@code redis.key.base64} holds
 * its exact bytes and {@code redis.key} shows it to people, with replacement characters. Every
 * processor reads a key through {@link #key(FlowFile)}, so none ever acts on the lossy form.
 */
public final class RedisBytes {

    public static final String KEY = "redis.key";
    public static final String KEY_BASE64 = "redis.key.base64";

    private RedisBytes() {
    }

    public static boolean isText(byte[] bytes) {
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    public static Map<String, String> keyAttributes(byte[] key) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(KEY, new String(key, StandardCharsets.UTF_8));
        if (!isText(key)) {
            attributes.put(KEY_BASE64, Base64.getEncoder().encodeToString(key));
        }
        return attributes;
    }

    /** The key a FlowFile names, or null when it names none. */
    public static byte[] key(FlowFile flowFile) {
        String base64 = flowFile.getAttribute(KEY_BASE64);
        if (base64 != null) {
            return Base64.getDecoder().decode(base64);
        }
        String key = flowFile.getAttribute(KEY);
        return key == null ? null : key.getBytes(StandardCharsets.UTF_8);
    }

    /** Equal exactly when the bytes are equal: for grouping and deduplicating by key. */
    public static String identity(byte[] key) {
        return new String(key, StandardCharsets.ISO_8859_1);
    }

    public static byte[] fromIdentity(String identity) {
        return identity.getBytes(StandardCharsets.ISO_8859_1);
    }

    /** For log lines and reasons: the key as text, with replacement characters where it is not text. */
    public static String display(byte[] key) {
        return new String(key, StandardCharsets.UTF_8);
    }
}
