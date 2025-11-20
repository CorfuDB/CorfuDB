package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class PersistentCorfuTable<K, V> implements
        ICorfuTable<K, V>,
        ICorfuSMR<ImmutableCorfuTable<K, V>> {

    private ICorfuSMRProxy<ImmutableCorfuTable<K, V>> proxy;

    private final Map<String, ICorfuSMRUpcallTarget<ImmutableCorfuTable<K, V>>> upcallTargetMap
        = ImmutableMap.<String, ICorfuSMRUpcallTarget<ImmutableCorfuTable<K, V>>>builder()
            .put("put", (obj, args) -> obj.put((K) args[0], (V) args[1]))
            .put("clear", (obj, args) -> obj.clear())
            .put("remove", (obj, args) -> obj.remove((K) args[0]))
            .build();

    public static <K, V> TypeToken<PersistentCorfuTable<K, V>> getTypeToken() {
        return new TypeToken<PersistentCorfuTable<K, V>>() {};
    }

    public PersistentCorfuTable() {}

    public PersistentCorfuTable(ICorfuSMRProxy<ImmutableCorfuTable<K, V>> proxy) {
        this.proxy = proxy;
    }

    @Override
    public void setCorfuSMRProxy(ICorfuSMRProxy<ImmutableCorfuTable<K, V>> proxy) {
        this.proxy = (ICorfuSMRProxy<ImmutableCorfuTable<K, V>>) proxy;
    }

    @Override
    public ICorfuSMRProxy<?> getCorfuSMRProxy() {
        return proxy;
    }

    @Override
    public void delete(@NonNull K key) {
        Object[] conflictField = new Object[]{key};
        proxy.logUpdate("remove", conflictField, key);
    }

    @Override
    public void insert(@NonNull K key, @NonNull V value) {
        Object[] conflictField = new Object[]{key};
        proxy.logUpdate("put", conflictField, key, value);
    }

    @Override
    public void clear() {
        proxy.logUpdate("clear", null);
    }

    @Override
    public void close() {
        proxy.close();
    }

    @Override
    public boolean isTableCached() {
        return proxy.isObjectCached();
    }

    @Override
    public V get(@Nonnull Object key) {
        Object[] conflictField = new Object[]{key};
        return proxy.access(corfuSmr -> corfuSmr.get((K)key), conflictField);
    }

    @Override
    public Set<K> keySet() {
        return proxy.access(ImmutableCorfuTable::keySet, null);
    }

    @Override
    public Stream<Map.Entry<K, V>> entryStream() {
        return proxy.access(ImmutableCorfuTable::entryStream, null);
    }

    @Override
    public boolean containsKey(@NonNull Object key) {
        Object[] conflictField = new Object[]{key};
        return proxy.access(corfuSmr -> corfuSmr.containsKey((K)key), conflictField);
    }

    @Override
    public int size() {
        return proxy.access(ImmutableCorfuTable::size, null);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public <I> Iterable<Map.Entry<K, V>> getByIndex(@NonNull final Index.Name indexName, I indexKey) {
        return proxy.access(corfuSmr -> corfuSmr.getByIndex(indexName, indexKey), null);
    }

    @Override
    public Map<String, ICorfuSMRUpcallTarget<ImmutableCorfuTable<K, V>>> getSMRUpcallMap() {
        return upcallTargetMap;
    }
}
