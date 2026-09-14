package io.dragonfly.nifi.redis.util;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.reporting.InitializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Minimal in-memory {@link DistributedMapCacheClient} test double for {@link PartitionAssignerTest}. */
public class FakeDistributedMapCacheClient implements DistributedMapCacheClient {

    private final Map<String, byte[]> store = new ConcurrentHashMap<>();

    @Override
    public synchronized <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        String k = serializeToString(key, keySerializer);
        if (store.containsKey(k)) {
            return false;
        }
        store.put(k, serializeToBytes(value, valueSerializer));
        return true;
    }

    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
        throw new UnsupportedOperationException("not used by PartitionAssigner");
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
        return store.containsKey(serializeToString(key, keySerializer));
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        store.put(serializeToString(key, keySerializer), serializeToBytes(value, valueSerializer));
    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        byte[] bytes = store.get(serializeToString(key, keySerializer));
        return valueDeserializer.deserialize(bytes);
    }

    @Override
    public void close() {
    }

    @Override
    public <K> boolean remove(K key, Serializer<K> keySerializer) throws IOException {
        return store.remove(serializeToString(key, keySerializer)) != null;
    }

    private <K> String serializeToString(K key, Serializer<K> serializer) throws IOException {
        return new String(serializeToBytes(key, serializer));
    }

    private <T> byte[] serializeToBytes(T value, Serializer<T> serializer) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serialize(value, out);
        return out.toByteArray();
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) throws InitializationException {
    }

    @Override
    public Collection<ValidationResult> validate(ValidationContext context) {
        return List.of();
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(String name) {
        return null;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return List.of();
    }

    @Override
    public String getIdentifier() {
        return "fake-cache";
    }
}
