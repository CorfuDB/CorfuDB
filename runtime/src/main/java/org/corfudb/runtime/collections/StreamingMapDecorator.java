package org.corfudb.runtime.collections;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A decorated map that provides additional functionality defined by the {@link StreamingMap}
 * interface. Whatever guarantees are being provided the the underlying map implementation
 * (such as ordering, retrieval complexity...) are also provided by this map.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class StreamingMapDecorator<K, V> implements StreamingMap<K, V> {

    final Map<K, V> mapImpl;

    public StreamingMapDecorator() {
        this(new HashMap<>());
    }

    StreamingMapDecorator(Map<K, V> mapImpl) {
        this.mapImpl = mapImpl;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Entry<K, V>> entryStream() {
        return mapImpl.entrySet().stream().parallel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return mapImpl.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return mapImpl.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(Object key) {
        return mapImpl.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(Object value) {
        return mapImpl.containsValue(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(Object key) {
        return mapImpl.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(K key, V value) {
        return mapImpl.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(Object key) {
        return mapImpl.remove(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        mapImpl.putAll(map);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        mapImpl.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<K> keySet() {
        return mapImpl.keySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<V> values() {
        return mapImpl.values();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return mapImpl.entrySet();
    }
}
