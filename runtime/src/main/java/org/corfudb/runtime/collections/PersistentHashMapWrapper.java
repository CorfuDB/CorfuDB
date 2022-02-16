package org.corfudb.runtime.collections;

import io.vavr.collection.HashMap;

public class PersistentHashMapWrapper<K, V> {

    HashMap<K, V> mapImpl;

    public PersistentHashMapWrapper() {
        mapImpl = HashMap.empty();
    }

    public void setMap(HashMap<K, V> map) {
        this.mapImpl = map;
    }

    public V get(K key) {
        return mapImpl.get(key).getOrElse((V) null);
    }

    public V put(K key, V value) {
        mapImpl = mapImpl.put(key, value);
        return mapImpl.get(key).get();
    }

    public V remove(K key) {
        V oldVal = mapImpl.get(key).getOrElse((V) null);
        mapImpl = mapImpl.remove(key);
        return oldVal;
    }

}
