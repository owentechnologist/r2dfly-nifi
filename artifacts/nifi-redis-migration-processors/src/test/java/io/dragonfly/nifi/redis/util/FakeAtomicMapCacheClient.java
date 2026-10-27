package io.dragonfly.nifi.redis.util;

import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** {@link FakeDistributedMapCacheClient} with compare-and-replace: every write bumps the key's revision. */
public class FakeAtomicMapCacheClient extends FakeDistributedMapCacheClient implements AtomicDistributedMapCacheClient<Long> {

    private final Map<String, Long> revisions = new ConcurrentHashMap<>();

    @Override
    public synchronized <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        boolean put = super.putIfAbsent(key, value, keySerializer, valueSerializer);
        if (put) {
            revisions.merge(text(key, keySerializer), 1L, Long::sum);
        }
        return put;
    }

    @Override
    public synchronized <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        super.put(key, value, keySerializer, valueSerializer);
        revisions.merge(text(key, keySerializer), 1L, Long::sum);
    }

    @Override
    public synchronized <K, V> AtomicCacheEntry<K, V, Long> fetch(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        V value = get(key, keySerializer, valueDeserializer);
        return value == null ? null : new AtomicCacheEntry<>(key, value, revisions.get(text(key, keySerializer)));
    }

    @Override
    public synchronized <K, V> boolean replace(AtomicCacheEntry<K, V, Long> entry, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        Long current = revisions.get(text(entry.getKey(), keySerializer));
        if (current == null || !current.equals(entry.getRevision().orElse(null))) {
            return false;
        }
        put(entry.getKey(), entry.getValue(), keySerializer, valueSerializer);
        return true;
    }

    private static <K> String text(K key, Serializer<K> serializer) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serialize(key, out);
        return out.toString();
    }
}
