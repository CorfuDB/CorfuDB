package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxyMetadata;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;
import org.corfudb.runtime.object.transactions.Transaction;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class PersistedCorfuTable<K, V> implements
        ICorfuTable<K, V>,
        ICorfuSMR<DiskBackedCorfuTable<K, V>> {

    private ICorfuSMRProxy<DiskBackedCorfuTable<K, V>> proxy;
    private ICorfuSMRProxyMetadata proxyMetadata;

    private final Map<String, ICorfuSMRUpcallTarget<DiskBackedCorfuTable<K, V>>> upcallTargetMap
            = ImmutableMap.<String, ICorfuSMRUpcallTarget<DiskBackedCorfuTable<K, V>>>builder()
            .put("put", (obj, args) -> obj.put((K) args[0], (V) args[1]))
            .put("clear", (obj, args) -> obj.clear())
            .put("remove", (obj, args) -> obj.remove((K) args[0]))
            .build();

    public static <K, V> TypeToken<PersistedCorfuTable<K, V>> getTypeToken() {
        return new TypeToken<PersistedCorfuTable<K, V>>() {};
    }

    public PersistedCorfuTable() {}

    public PersistedCorfuTable(ICorfuSMRProxy<DiskBackedCorfuTable<K, V>> proxy,
                               ICorfuSMRProxyMetadata proxyMetadata) {
        this.proxy = proxy;
        this.proxyMetadata = proxyMetadata;
    }

    @Override
    public ICorfuSMRProxyMetadata getCorfuSMRProxy() {
        return proxyMetadata;
    }

    @Override
    public <P extends ICorfuSMRProxyMetadata & ICorfuSMRProxy<DiskBackedCorfuTable<K, V>>>
    void setCorfuSMRProxy(P proxy) {
        this.proxy = proxy;
        this.proxyMetadata = proxy;
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
        proxy.getUnderlyingMVO().close();
    }

    @Override
    public boolean isTableCached() {
        return proxyMetadata.isObjectCached();
    }

    @Override
    public ICorfuTable<K, V> generateImmutableView(long sequence) {
        ICorfuSMRProxy<DiskBackedCorfuTable<K, V>> snapshotProxy =
                this.proxy.getUnderlyingMVO().getSnapshotProxy(sequence);
        return new PersistedCorfuTable<>(snapshotProxy, this.proxyMetadata);
    }

    @Override
    public V get(@Nonnull Object key) {
        Object[] conflictField = new Object[]{key};
        return proxy.access(corfuSmr -> corfuSmr.get(key), conflictField);
    }

    @Override
    @Deprecated
    public Set<K> keySet() {
        log.warn("keySet() is deprecated, please use entryStream() instead.");
        return entryStream().map(Map.Entry::getKey).collect(Collectors.toSet());
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
    public <I> Iterable<Map.Entry<K, V>> getByIndex(@NonNull final Index.Name indexName, @NonNull I indexKey) {
        return proxy.access(corfuSmr -> corfuSmr.getByIndex(indexName, indexKey), null);
    }
    @Override
    public Map<String, ICorfuSMRUpcallTarget<DiskBackedCorfuTable<K, V>>> getSMRUpcallMap() {
        return upcallTargetMap;
    }
}
