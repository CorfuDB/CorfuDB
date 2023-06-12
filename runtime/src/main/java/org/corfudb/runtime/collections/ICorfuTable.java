package org.corfudb.runtime.collections;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

public interface ICorfuTable<K, V> {

    /** Insert a key-value pair into a map, overwriting any previous mapping.
     *
     * This function performs better than put, because no value
     * is returned (and is therefore a write rather than a read-modify-write).
     *
     * @param key       The key to insert
     * @param value     The value to insert
     */
    void insert(K key, V value);

    /** Delete a key from a map.
     *
     * This function performs better than a remove, because no value
     * is returned (and is therefore a write rather than a read-modify-write).
     *
     * @param key       The key to delete
     */
    void delete(K key);

    /**
     * Get a mapping using the specified index function.
     *
     * @param indexName Name of the secondary index to query.
     * @param indexKey  The index key used to query the secondary index
     * @return An Iterable of Map.Entry<K, V>
     */
    <I> Iterable<Entry<K, V>> getByIndex(@Nonnull final Index.Name indexName, I indexKey);

    V get(Object key);

    Set<K> keySet();

    Stream<Entry<K, V>> entryStream();

    boolean containsKey(Object key);

    int size();

    boolean isEmpty();

    void clear();

    void close();

    boolean isTableCached();

    ICorfuTable<K, V> generateImmutableView(long sequence);
}
