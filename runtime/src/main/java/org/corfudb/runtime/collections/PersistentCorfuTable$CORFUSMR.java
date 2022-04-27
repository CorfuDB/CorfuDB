package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class PersistentCorfuTable$CORFUSMR<K, V> extends PersistentCorfuTable<K, V> implements ICorfuSMR<PersistentCorfuTable<K, V>> {

    public ICorfuSMRProxy<PersistentCorfuTable<K, V>> proxy_CORFUSMR;

    public final Map<String, ICorfuSMRUpcallTarget<PersistentCorfuTable<K, V>>> upcallMap_CORFUSMR =
            new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<PersistentCorfuTable<K, V>>>()
            .put("put", (obj, args) -> { return obj.put((K) args[0], (V) args[1]);})
            .put("clear", (obj, args) -> { obj.clear();return null;})
            .put("remove", (obj, args) -> { return obj.remove((K) args[0]);}).build();

    public PersistentCorfuTable$CORFUSMR() {
        super();
    }

    public ICorfuSMRProxy<PersistentCorfuTable<K, V>> getCorfuSMRProxy() {
        return proxy_CORFUSMR;
    }

    public void setCorfuSMRProxy(ICorfuSMRProxy<PersistentCorfuTable<K, V>> proxy) {
        this.proxy_CORFUSMR = proxy;
    }

    @Override
    public V remove(@ConflictParameter K key) {
        Object[] conflictField_CORFUSMR = new Object[]{key};
        long address_CORFUSMR = proxy_CORFUSMR.logUpdate("remove",true,conflictField_CORFUSMR,key);
        return (V) proxy_CORFUSMR.getUpcallResult(address_CORFUSMR, conflictField_CORFUSMR);
    }

    @Override
    public void delete(@ConflictParameter K key) {
        Object[] conflictField_CORFUSMR = new Object[]{key};
        proxy_CORFUSMR.logUpdate("remove",false, conflictField_CORFUSMR, key);
    }

    @Override
    public V put(@ConflictParameter K key, V value) {
        Object[] conflictField_CORFUSMR = new Object[]{key};
        long address_CORFUSMR = proxy_CORFUSMR.logUpdate("put",true,conflictField_CORFUSMR,key, value);
        return (V) proxy_CORFUSMR.getUpcallResult(address_CORFUSMR, conflictField_CORFUSMR);
    }

    @Override
    public void insert(@ConflictParameter K key, V value) {
        Object[] conflictField_CORFUSMR = new Object[]{key};
        proxy_CORFUSMR.logUpdate("put",false, conflictField_CORFUSMR, key, value);
    }

    @Override
    public V get(@ConflictParameter K key) {
        Object[] conflictField_CORFUSMR = new Object[]{key};
        return proxy_CORFUSMR.access(o_CORFUSMR -> o_CORFUSMR.get(key),conflictField_CORFUSMR);
    }

    @Override
    public Set<K> keySet() {
        return proxy_CORFUSMR.access(PersistentCorfuTable::keySet, null);
    }

    @Override
    public Stream<Map.Entry<K, V>> entryStream() {
        return proxy_CORFUSMR.access(PersistentCorfuTable::entryStream, null);
    }

    @Override
    public boolean containsKey(@ConflictParameter K key) {
        Object[] conflictField_CORFUSMR = new Object[]{key};
        return proxy_CORFUSMR.access(o_CORFUSMR -> o_CORFUSMR.containsKey(key), conflictField_CORFUSMR);
    }

    @Override
    public int size() {
        return proxy_CORFUSMR.access(PersistentCorfuTable::size,null);
    }

    @Override
    public boolean isEmpty() {
        return proxy_CORFUSMR.access(PersistentCorfuTable::isEmpty, null);
    }

    @Override
    public void clear() {
        proxy_CORFUSMR.logUpdate("clear", false, null);
    }

    @Override
    public <I>Collection<Map.Entry<K, V>> getByIndex(@Nonnull final Index.Name indexName, I indexKey) {
        return proxy_CORFUSMR.access(o_CORFUSMR -> o_CORFUSMR.getByIndex(indexName, indexKey), null);
    }

    public Map<String, ICorfuSMRUpcallTarget<PersistentCorfuTable<K, V>>> getCorfuSMRUpcallMap() {
        return upcallMap_CORFUSMR;
    }
}
