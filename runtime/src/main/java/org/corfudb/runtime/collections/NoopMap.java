package org.corfudb.runtime.collections;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class NoopMap<K, V> implements ContextAwareMap<K, V>{
    @Override
    public Stream<Entry<K, V>> entryStream() {
        throw new IllegalStateException();
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        return null;
    }

    @Override
    public V put(K key, V value) {
        return null;
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
    }

    @Override
    public void clear() {
    }

    @Override
    public Set<K> keySet() {
        throw new IllegalStateException();
    }

    @Override
    public Collection<V> values() {
        throw new IllegalStateException();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new IllegalStateException();
    }
}
