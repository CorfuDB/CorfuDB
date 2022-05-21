package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import org.corfudb.runtime.object.ICorfuExecutionContext;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.stream.Stream;

public class PersistentCorfuTable<K, V> implements IPersistentCorfuTable<K, V>, ICorfuSMR<PersistentCorfuTable<K, V>> {

    protected static final ForkJoinPool pool = new ForkJoinPool(Math.max(Runtime.getRuntime().availableProcessors() - 1, 1),
            pool -> {
                final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                worker.setName("PersistentCorfuTable-Forkjoin-pool-" + worker.getPoolIndex());
                return worker;
            }, null, true);

    private ICorfuSMRProxy<ImmutableCorfuTable<K, V>> proxy;

    private final Map<String, ICorfuSMRUpcallTarget<ImmutableCorfuTable<K, V>>> upcallTargetMap
        = ImmutableMap.<String, ICorfuSMRUpcallTarget<ImmutableCorfuTable<K, V>>>builder()
            .put("put", (obj, args) -> obj.put((K) args[0], (V) args[1]))
            .put("clear", (obj, args) -> obj.clear())
            .put("remove", (obj, args) -> obj.remove((K) args[0]))
            .build();

    @Override
    public <R> void setProxy$CORFUSMR(ICorfuSMRProxy<R> proxy) {
        this.proxy = (ICorfuSMRProxy<ImmutableCorfuTable<K, V>>) proxy;
    }

    @Override
    // TODO: use proper return type
    public ICorfuSMRProxy getCorfuSMRProxy() {
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
    public V get(@Nonnull K key) {
        Object[] conflictField = new Object[]{key};
        return proxy.access(corfuSmr -> corfuSmr.get(key), conflictField);
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
    public boolean containsKey(@Nonnull K key) {
        Object[] conflictField = new Object[]{key};
        return proxy.access(corfuSmr -> corfuSmr.containsKey(key), conflictField);
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
    public <I>Collection<Map.Entry<K, V>> getByIndex(@Nonnull final Index.Name indexName, I indexKey) {
        return proxy.access(corfuSmr -> corfuSmr.getByIndex(indexName, indexKey), null);
    }

    @Override
    public PersistentCorfuTable<K, V> getContext(ICorfuExecutionContext.Context context) {
        return null;
    }

    @Override
    public Map<String, ICorfuSMRUpcallTarget<ImmutableCorfuTable<K, V>>> getSMRUpcallMap() {
        return upcallTargetMap;
    }

    @Override
    public UUID getCorfuStreamID() {
        return proxy.getStreamID();
    }
}

/**
public class PersistentCorfuTable<K, V> implements ICorfuSMR<PersistentCorfuTable<K, V>> {

    private PersistentHashMapWrapper<K, V> tableState;

    public PersistentCorfuTable() {
        tableState = new PersistentHashMapWrapper<>();
    }

    public PersistentCorfuTable(@Nonnull final Index.Registry<K, V> indices) {
        tableState = new PersistentHashMapWrapper<>(indices);
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
**/
