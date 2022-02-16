package org.corfudb.runtime.collections;

import io.vavr.collection.HashMap;
import org.corfudb.runtime.object.ICorfuSMR;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class PersistentCorfuTable<K, V> implements ICorfuTable<K, V>, ICorfuSMR<PersistentCorfuTable<K, V>> {

    private PersistentHashMapWrapper<K, V> mainMap;

    public PersistentCorfuTable() {
        mainMap = new PersistentHashMapWrapper<>();
    }

    @Override
    public void setImmutableObject(Object obj) {
        mainMap.setMap((HashMap)obj);
    }

    @Override
    public Object getImmutableObject() {
        return mainMap;
    }

    @Override
    public void insert(K key, V value) {
        // see PersistentCorfuTable$CORFUSMR
    }

    @Override
    public void delete(K key) {
        // see PersistentCorfuTable$CORFUSMR
    }

    @Override
    public List<V> scanAndFilter(Predicate<? super V> valuePredicate) {
        return null;
    }

    @Override
    public Collection<Entry<K, V>> scanAndFilterByEntry(Predicate<? super Entry<K, V>> entryPredicate) {
        return null;
    }

    @Override
    public Stream<Entry<K, V>> entryStream() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
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
        return mainMap.get((K)key);
    }

    @Override
    public V put(K key, V value) {
        return mainMap.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return mainMap.remove((K)key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<K> keySet() {
        return null;
    }

    @Override
    public Collection<V> values() {
        return null;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }


    @Override
    public PersistentCorfuTable<K, V> getContext(Context context) {
        return this;
    }
}
