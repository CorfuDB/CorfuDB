package org.corfudb.runtime.collections;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

// TODO: Update usages to ICorfuTable
public interface IPersistentCorfuTable<K, V> {

    void delete(@Nonnull K key);

    void insert(@Nonnull K key, @Nonnull V value);

    V get(@Nonnull K key);

    Set<K> keySet();

    Stream<Map.Entry<K, V>> entryStream();

    boolean containsKey(@Nonnull K key);

    int size();

    boolean isEmpty();

    void clear();

    <I> Collection<Map.Entry<K, V>> getByIndex(@Nonnull final Index.Name indexName, I indexKey);
}
