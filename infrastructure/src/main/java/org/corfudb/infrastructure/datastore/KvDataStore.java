package org.corfudb.infrastructure.datastore;


import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Optional;

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
     * Retrieves the value for a key under a prefix.
     *
     * @param key record meta information
     * @return value stored under key
     */
    default <T> Optional<T> find(KvRecord<T> key) {
        return Optional.ofNullable(get(key));
    }

    /**
     * Retrieves the value for a key or a default value
     *
     * @param key key meta info
     * @param defaultValue a default value
     * @param <T> value type
     * @return value or a default value
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

        /**
         * Build kv record
         * @param prefix prefix
         * @param key key name
         * @param dataType dataType
         * @param <R> class type
         * @return kv record
         */
        public static <R> KvRecord<R> of(String prefix, String key, Class<R> dataType) {
            return new KvRecord<>(prefix, key, dataType);
        }

        /**
         * Build kv record with empty prefix
         * @param key key name
         * @param dataType data type
         * @param <R> class type
         * @return kv record
         */
        public static <R> KvRecord<R> of(String key, Class<R> dataType) {
            return of("", key, dataType);
        }

        public String getFullKeyName() {
            return prefix + "_" + key;
        }
    }
}
