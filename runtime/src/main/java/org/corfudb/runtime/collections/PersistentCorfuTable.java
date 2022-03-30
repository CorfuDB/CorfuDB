package org.corfudb.runtime.collections;

import lombok.NonNull;
import org.corfudb.runtime.object.ICorfuSMR;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class PersistentCorfuTable<K, V> implements ICorfuSMR<PersistentCorfuTable<K, V>> {

    private PersistentHashMapWrapper<K, V> tableState;

    public PersistentCorfuTable() {
        tableState = new PersistentHashMapWrapper<>();
    }

    @Override
    public void setImmutableState(Object obj) {
        tableState = (PersistentHashMapWrapper<K, V>) obj;
    }

    @Override
    public Object getImmutableState() {
        return tableState;
    }

    public void insert(@NonNull K key, @NonNull V value) {
        // see PersistentCorfuTable$CORFUSMR
    }

    public void delete(@NonNull K key) {
        // see PersistentCorfuTable$CORFUSMR
    }

    // TODO: Stream<Entry<K, V>> entryStream()

    public int size() {
        return tableState.size();
    }

    public boolean isEmpty() {
        return tableState.size() == 0;
    }

    public boolean containsKey(Object key) {
        return false;
    }

    public V get(@NonNull K key) {
        return tableState.get(key);
    }

    public V put(@NonNull K key, @NonNull V value) {
        final V prev = tableState.get(key);
        tableState = tableState.put(key, value);
        return prev;
    }

    public V remove(@NonNull K key) {
        final V value = tableState.get(key);
        tableState = tableState.remove(key);
        return value;
    }

    public void clear() {

    }

    public Set<K> keySet() {
        return null;
    }

    @Override
    public PersistentCorfuTable<K, V> getContext(Context context) {
        return this;
    }

    @Override
    public void reset() {
        tableState = new PersistentHashMapWrapper<>();
    }
}
