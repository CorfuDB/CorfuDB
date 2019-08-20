package org.corfudb.infrastructure.datastore;


import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Key Value data store abstraction that provides persistence for variables that need
 * retain values across node restarts or need to be accessed by multiple modules/threads.
 *
 * <p>The key value store is partitioned by prefix (namespace/table). All values being stored
 * under a prefix should be of a single Type or Class T.
 *
 * <p>Created by mdhawan on 7/27/16.
 */
public interface KvDataStore {

    @Deprecated
    <T> void put(Class<T> tclass, String prefix, String key, T value);

    @Deprecated
    <T> T get(Class<T> tclass, String prefix, String key);

    @Deprecated
    <T> void delete(Class<T> tclass, String prefix, String key);

    /**
     * Stores a value for a key under a prefix (namespace).
     *
     * @param key record meta information
     * @param value  Immutable value (or a value that won't be changed)
     */
    <T> void put(KvRecord<T> key, T value);

    /**
     * Retrieves the value for a key under a prefix.
     *
     * @param key record meta information
     * @return value stored under key
     */
    <T> T get(KvRecord<T> key);

    /**
     * Retrieves the value for a key or a default value
     *
     * @param key key meta info
     * @param defaultValue a default value
     * @param <T>
     * @return
     */
    <T> T get(KvRecord<T> key, T defaultValue);

    /**
     * Deletes the value for a key under a prefix.
     *
     * @param key record meta information
     */
    <T> void delete(KvRecord<T> key);

    /**
     * Key-value meta information class, provides all the information for saving and getting data from a data store
     *
     * @param <T> data type
     */
    @AllArgsConstructor
    @Getter
    class KvRecord<T> {
        /**
         * namespace prefix for a key
         */
        private final String prefix;
        /**
         * key in a data store
         */
        private final String key;
        /**
         * The class of the value in a data store
         */
        private final Class<T> dataType;

        public String getFullKeyName() {
            return prefix + "_" + key;
        }
    }
}
