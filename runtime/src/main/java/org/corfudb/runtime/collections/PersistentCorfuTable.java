package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxyMetadata;
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
    private ICorfuSMRProxyMetadata proxyMetadata;


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

    public PersistentCorfuTable(ICorfuSMRProxy<ImmutableCorfuTable<K, V>> proxy,
                                ICorfuSMRProxyMetadata proxyMetadata) {
        this.proxy = proxy;
        this.proxyMetadata = proxyMetadata;
    }

    @Override
    public <P extends ICorfuSMRProxyMetadata & ICorfuSMRProxy<ImmutableCorfuTable<K, V>>>
    void setCorfuSMRProxy(P proxy) {
        this.proxy = proxy;
        this.proxyMetadata = proxy;
    }

    @Override
    public ICorfuSMRProxyMetadata getCorfuSMRProxy() {
        return proxyMetadata;
    }

    @Override
    public void delete(@Nonnull K key) {
        Object[] conflictField = new Object[]{key};
        proxy.logUpdate("remove", conflictField, key);
    }

    @Override
    public void insert(@Nonnull K key, @Nonnull V value) {
        Object[] conflictField = new Object[]{key};
        proxy.logUpdate("put", conflictField, key, value);
    }

    @Override
    public void clear() {
        proxy.logUpdate("clear", null);
    }

    @Override
    public void close() {
        // NO OP
    }

    @Override
    public boolean isTableCached() {
        return proxyMetadata.isObjectCached();
    }

    @Override
    public ICorfuTable<K, V> generateImmutableView(long sequence) {
        ICorfuSMRProxy<ImmutableCorfuTable<K, V>> snapshotProxy =
                this.proxy.getUnderlyingMVO().getSnapshotProxy(sequence);
        return new PersistentCorfuTable<>(snapshotProxy, this.proxyMetadata);
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
    public boolean containsKey(@Nonnull Object key) {
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
    public <I> Iterable<Map.Entry<K, V>> getByIndex(@Nonnull final Index.Name indexName, I indexKey) {
        return proxy.access(corfuSmr -> corfuSmr.getByIndex(indexName, indexKey), null);
    }

    @Override
    public Map<String, ICorfuSMRUpcallTarget<ImmutableCorfuTable<K, V>>> getSMRUpcallMap() {
        return upcallTargetMap;
    }
}
