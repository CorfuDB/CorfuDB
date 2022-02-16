package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.object.ICorfuSMRUpcallTarget;

import java.util.Map;

public class PersistentCorfuTable$CORFUSMR<K, V> extends PersistentCorfuTable<K, V> implements ICorfuSMR<PersistentCorfuTable<K, V>> {

    public ICorfuSMRProxy<PersistentCorfuTable<K, V>> proxy_CORFUSMR;

    public final Map<String, ICorfuSMRUpcallTarget<PersistentCorfuTable<K, V>>> upcallMap_CORFUSMR =
            new ImmutableMap.Builder<String, ICorfuSMRUpcallTarget<PersistentCorfuTable<K, V>>>()
            .put("put", (obj, args) -> { return obj.put((K) args[0], (V) args[1]);})
            .put("clear", (obj, args) -> { obj.clear();return null;})
            .put("remove", (obj, args) -> { return obj.remove((java.lang.Object) args[0]);}).build();

    public PersistentCorfuTable$CORFUSMR() {
        super();
    }

    public ICorfuSMRProxy<PersistentCorfuTable<K, V>> getCorfuSMRProxy() {
        return proxy_CORFUSMR;
    }

    public void setCorfuSMRProxy(ICorfuSMRProxy<PersistentCorfuTable<K, V>> proxy) {
        this.proxy_CORFUSMR = proxy;
    }

    public V remove(@ConflictParameter Object key) {
        Object[] conflictField_CORFUSMR = new Object[]{key};
        long address_CORFUSMR = proxy_CORFUSMR.logUpdate("remove",true,conflictField_CORFUSMR,key);
        return (V) proxy_CORFUSMR.getUpcallResult(address_CORFUSMR, conflictField_CORFUSMR);
    }


    public V put(@ConflictParameter K key, V value) {
        Object[] conflictField_CORFUSMR = new Object[]{key};
        long address_CORFUSMR = proxy_CORFUSMR.logUpdate("put",true,conflictField_CORFUSMR,key, value);
        return (V) proxy_CORFUSMR.getUpcallResult(address_CORFUSMR, conflictField_CORFUSMR);
    }

    public V get(@ConflictParameter Object key) {
        Object[] conflictField_CORFUSMR = new Object[]{key};
        return proxy_CORFUSMR.access(o_CORFUSMR -> o_CORFUSMR.get(key),conflictField_CORFUSMR);
    }

    public Map<String, ICorfuSMRUpcallTarget<PersistentCorfuTable<K, V>>> getCorfuSMRUpcallMap() {
        return upcallMap_CORFUSMR;
    }

}
