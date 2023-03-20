package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class PersistedCorfuTable<K, V> implements ICorfuTable<K, V> {

    private MVOCorfuCompileProxy<PersistedCorfuTable<K, V>, DiskBackedCorfuTable<K, V>> proxy;

    private final Map<String, ICorfuSMRUpcallTarget<DiskBackedCorfuTable<K, V>>> upcallTargetMap
            = ImmutableMap.<String, ICorfuSMRUpcallTarget<DiskBackedCorfuTable<K, V>>>builder()
            .put("put", (obj, args) -> obj.put((K) args[0], (V) args[1]))
            .put("clear", (obj, args) -> obj.clear())
            .put("remove", (obj, args) -> obj.remove((K) args[0]))
            .build();

    public static <K, V> TypeToken<PersistedCorfuTable<K, V>> getTypeToken() {
        return new TypeToken<PersistedCorfuTable<K, V>>() {};
    }

    @Override
    public void setCorfuSMRProxy(MVOCorfuCompileProxy<?, ?> proxy) {
        this.proxy = (MVOCorfuCompileProxy<PersistedCorfuTable<K, V>, DiskBackedCorfuTable<K, V>>) proxy;
    }

    @Override
    public MVOCorfuCompileProxy<?, ?> getCorfuSMRProxy() {
        return proxy;
    }

    @Override
    public void delete(@Nonnull K key) {
        Object[] conflictField = new Object[]{key};
        proxy.logUpdate("remove", false, conflictField, key);
    }

    @Override
    public void insert(@Nonnull K key, @Nonnull V value) {
        Object[] conflictField = new Object[]{key};
        proxy.logUpdate("put", false, conflictField, key, value);
    }

    @Override
    public void clear() {
        proxy.logUpdate("clear", false, null);
    }

    @Override
    public void close() {
        proxy.getUnderlyingMVO().getCurrentObject().close();
    }

    @Override
    public boolean isTableCached() {
        return proxy.isObjectCached();
    }

    @Override
    public V get(@Nonnull Object key) {
        Object[] conflictField = new Object[]{key};
        return proxy.access(corfuSmr -> corfuSmr.get(key), conflictField);
    }

    @Override
    public Set<K> keySet() {
        // We only allow operations over streams and iterators.
        throw new UnsupportedOperationException("Please use entryStream() API");
    }

    @Override
    public Stream<Map.Entry<K, V>> entryStream() {
        return proxy.access(DiskBackedCorfuTable::entryStream, null);
    }

    @Override
    public boolean containsKey(@Nonnull Object key) {
        Object[] conflictField = new Object[]{key};
        return proxy.access(corfuSmr -> corfuSmr.containsKey(key), conflictField);
    }

    @Override
    public int size() {
        return proxy.access(DiskBackedCorfuTable::size, null).intValue();
    }

    @Override
    public boolean isEmpty() {
        return proxy.access(DiskBackedCorfuTable::size, null) == 0;
    }

    @Override
    public <I> Iterable<Map.Entry<K, V>> getByIndex(@Nonnull final Index.Name indexName, I indexKey) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }
    @Override
    public Map<String, ICorfuSMRUpcallTarget<DiskBackedCorfuTable<K, V>>> getSMRUpcallMap() {
        return upcallTargetMap;
    }
}
