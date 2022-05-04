package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.object.ICorfuSMR;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class PersistentCorfuTable<K, V> implements ICorfuSMR<PersistentCorfuTable<K, V>> {

    private PersistentHashMapWrapper<K, V> tableState;

    public PersistentCorfuTable() {
        tableState = new PersistentHashMapWrapper<>();
    }

    public PersistentCorfuTable(@Nonnull final Index.Registry<K, V> indices) {
        tableState = new PersistentHashMapWrapper<>(indices);
    }

    public PersistentCorfuTable(PersistentHashMapWrapper<K, V> tableState) {
        this.tableState = tableState;
    }

    @Override
    public void setImmutableState(Object obj) {
        tableState = (PersistentHashMapWrapper<K, V>) obj;
    }

    @Override
    public Object getImmutableState() {
        return tableState;
    }

    public void insert(@Nonnull K key, @Nonnull V value) {
        // See PersistentCorfuTable$CORFUSMR
        // tableState = tableState.put(key, value);
    }

    public void delete(@Nonnull K key) {
        // See PersistentCorfuTable$CORFUSMR
        // tableState = tableState.remove(key);
    }

    public Stream<Map.Entry<K, V>> entryStream() {
        return tableState.entryStream();
    }

    public int size() {
        return tableState.size();
    }

    public boolean isEmpty() {
        return tableState.size() == 0;
    }

    public boolean containsKey(@Nonnull K key) {
        return tableState.containsKey(key);
    }

    public V get(@Nonnull K key) {
        return tableState.get(key);
    }

    @Deprecated
    public V put(@Nonnull K key, @Nonnull V value) {
        final V prev = tableState.get(key);
        tableState = tableState.put(key, value);
        return prev;
    }

    @Deprecated
    public V remove(@Nonnull K key) {
        final V value = tableState.get(key);
        tableState = tableState.remove(key);
        return value;
    }

    public void clear() {
        tableState = tableState.clear();
    }

    public Set<K> keySet() {
        return tableState.keySet();
    }

    public <I> Collection<Map.Entry<K, V>> getByIndex(@Nonnull final Index.Name indexName, I indexKey) {
        return tableState.getByIndex(indexName, indexKey);
    }

    @Override
    public PersistentCorfuTable<K, V> getContext(Context context) {
        return this;
    }

    @Override
    public void reset() {
        tableState = tableState.clear();
    }

    public static <K, V> TypeToken<PersistentCorfuTable<K, V>> getTableType() {
        return new TypeToken<PersistentCorfuTable<K, V>>() {};
    }
}
