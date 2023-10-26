package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.Options;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@Slf4j
public class ExtensibleCache<K, V> implements
        ICorfuTable<K, V>, AutoCloseable {

    private final DiskBackedCorfuTable<K, V> table;

    public static <K, V> TypeToken<ExtensibleCache<K, V>> getTypeToken() {
        return new TypeToken<ExtensibleCache<K, V>>() {};
    }

    public ExtensibleCache(@NonNull PersistenceOptions persistenceOptions,
                                @NonNull Options rocksDbOptions,
                                @NonNull ISerializer serializer,
                                @Nonnull Index.Registry<K, V> indices) {
        this.table = new DiskBackedCorfuTable<>(persistenceOptions, rocksDbOptions, serializer, indices);
    }

    @Override
    public void delete(@Nonnull K key) {
        table.remove(key);
    }

    @Override
    public void insert(@Nonnull K key, @Nonnull V value) {
        table.put(key, value);
    }

    @Override
    public void clear() {
        table.clear();
    }

    @Override
    public void close() {
        table.close();
    }

    @Override
    public boolean isTableCached() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(@Nonnull Object key) {
        return table.get(key);
    }

    @Override
    @Deprecated
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<Map.Entry<K, V>> entryStream() {
        return table.entryStream();
    }

    @Override
    public boolean containsKey(@Nonnull Object key) {
        return table.containsKey(key);
    }

    @Override
    public int size() {
        return (int) table.size();
    }

    @Override
    public boolean isEmpty() {
        return table.size() == 0;
    }

    @Override
    public <I> Iterable<Map.Entry<K, V>> getByIndex(@NonNull final Index.Name indexName, @NonNull I indexKey) {
        throw new UnsupportedOperationException();
    }
}
