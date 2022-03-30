package org.corfudb.runtime.collections;

import io.vavr.collection.HashMap;
import lombok.NonNull;

public class PersistentHashMapWrapper<K, V> {

    // The "main" map which contains the primary key-value mappings.
    HashMap<K, V> mainMap;

    // TODO: add secondary indices and other immutable versioned state associated with the PersistentCorfuTable

    public PersistentHashMapWrapper() {
        this.mainMap = HashMap.empty();
    }

    /**
     * Internal constructor used to produce new copies/versions.
     * @param mainMap
     */
    private PersistentHashMapWrapper(@NonNull final HashMap<K, V> mainMap) {
        this.mainMap = mainMap;
    }

    /**
     *
     * @param key
     * @return
     */
    public V get(@NonNull K key) {
        return mainMap.get(key).getOrElse((V) null);
    }

    /**
     *
     * @param key
     * @param value
     * @return
     */
    public PersistentHashMapWrapper<K, V> put(@NonNull K key, @NonNull V value) {
        return new PersistentHashMapWrapper<>(mainMap.put(key, value));
    }

    /**
     *
     * @param key
     * @return
     */
    public PersistentHashMapWrapper<K, V> remove(@NonNull K key) {
        // TODO: does removing a non-existing entry produce a new version?
        return new PersistentHashMapWrapper<>(mainMap.remove(key));
    }

    /**
     *
     * @return
     */
    public int size() {
        return mainMap.size();
    }
}
