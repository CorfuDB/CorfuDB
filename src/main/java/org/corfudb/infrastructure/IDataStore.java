package org.corfudb.infrastructure;

import java.util.List;

/**
 * Key Value data store abstraction that provides persistence for variables that need
 * retain values across node restarts or need to be accessed by multiple modules/threads.
 * <p>
 * The key value store is partitioned by prefix (namespace/table). All values being stored
 * under a prefix should be of a single Type or Class<T>.
 * <p>
 * <p>
 * Created by mdhawan on 7/27/16.
 */
public interface IDataStore {
    /**
     * Stores a value for a key under a prefix (namespace).
     *
     * @param tClass the class of the object being stored
     * @param prefix
     * @param key
     * @param value
     * @param <T>
     */
    public <T> void put(Class<T> tClass, String prefix, String key, T value);

    /**
     * Retrieves the value for a key under a prefix.
     *
     * @param tClass the class of the object being retrieved
     * @param prefix
     * @param key
     * @param <T>
     * @return
     */
    public <T> T get(Class<T> tClass, String prefix, String key);

    /**
     * Deletes the value for a key under a prefix.
     *
     * @param tClass the class of the object being retrieved
     * @param prefix
     * @param key
     * @param <T>
     */
    public <T> void delete(Class<T> tClass, String prefix, String key);

    /**
     * Retrieves all the values under a prefix.
     * <p>
     * NOTE there is no ordered retrieval provided.
     *
     * @param tClass the class of the objects being retrieved.
     * @param prefix
     * @param <T>
     * @return
     */
    public <T> List<T> getAll(Class<T> tClass, String prefix);

}
