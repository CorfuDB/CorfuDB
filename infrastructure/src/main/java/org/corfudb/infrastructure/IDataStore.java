package org.corfudb.infrastructure;

import java.util.List;

/**
 * Key Value data store abstraction that provides persistence for variables that need
 * retain values across node restarts or need to be accessed by multiple modules/threads.
 *
 * <p>The key value store is partitioned by prefix (namespace/table). All values being stored
 * under a prefix should be of a single Type or Class T.
 *
 * <p>Created by mdhawan on 7/27/16.
 */
public interface IDataStore {
    /**
     * Stores a value for a key under a prefix (namespace).
     *
     * @param tclass the class of the object being stored
     * @param prefix namespace
     * @param key    key-value key to store into
     * @param value  key-value value
     */
    public <T> void put(Class<T> tclass, String prefix, String key, T value);

    /**
     * Retrieves the value for a key under a prefix.
     *
     * @param tclass the class of the object being retrieved
     * @param prefix namespace
     * @param key    key-value key to look up
     * @return value stored under key
     */
    public <T> T get(Class<T> tclass, String prefix, String key);

    /**
     * Deletes the value for a key under a prefix.
     *
     * @param tclass the class of the object being retrieved
     * @param prefix namespace
     * @param key    key-value key to delete
     */
    public <T> void delete(Class<T> tclass, String prefix, String key);

    /**
     * Retrieves all the values under a prefix.
     *
     * <p>NOTE there is no ordered retrieval provided.
     *
     * @param tclass the class of the objects being retrieved.
     * @param prefix namespace
     * @return list of all values stored under namespace
     */
    public <T> List<T> getAll(Class<T> tclass, String prefix);

}
