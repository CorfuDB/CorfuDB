package org.corfudb.runtime.collections;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public interface ICorfuTable<K, V> extends StreamingMap<K, V> {

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
     * Returns a filtered {@link List} view of the values contained in this map.
     * This method has a memory/CPU advantage over the map iterators as no deep copy
     * is actually performed.
     *
     * @param valuePredicate java predicate (function to evaluate)
     * @return a view of the values contained in this map meeting the predicate condition.
     */
    List<V> scanAndFilter(Predicate<? super V> valuePredicate);

    /**
     * Returns a {@link Collection} filtered by entries (keys and/or values).
     * This method has a memory/CPU advantage over the map iterators as no deep copy
     * is actually performed.
     *
     * @param entryPredicate java predicate (function to evaluate)
     * @return a view of the entries contained in this map meeting the predicate condition.
     */
    Collection<Entry<K, V>> scanAndFilterByEntry(
            Predicate<? super Map.Entry<K, V>> entryPredicate);
}
