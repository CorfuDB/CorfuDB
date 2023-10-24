package org.corfudb.runtime.collections.cache;

import org.corfudb.runtime.collections.DiskBackedCorfuTable;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.RocksDbStore;
import org.corfudb.runtime.object.RocksDbStore.IndexMode;
import org.corfudb.runtime.object.RocksDbStore.StoreMode;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

public class ExtensibleCache<K, V> implements AutoCloseable {
    private final Options defaultOptions = new Options().setCreateIfMissing(true);
    private final RocksDbStore<DiskBackedCorfuTable<K, V>> rocksDbStore;
    private final DiskBackedCorfuTable<K, V> table;

    public ExtensibleCache(PersistenceOptions persistenceOptions){
        this(persistenceOptions, Serializers.getDefaultSerializer());
    }

    public ExtensibleCache(PersistenceOptions persistenceOptions, ISerializer serializer) {
        try {
            rocksDbStore = new RocksDbStore<>(
                    persistenceOptions.getDataPath(), defaultOptions,
                    DiskBackedCorfuTable.WRITE_OPTIONS, persistenceOptions, StoreMode.PERSISTENT, IndexMode.NON_INDEX
            );
            table = new DiskBackedCorfuTable<>(serializer, rocksDbStore, Optional.empty());

        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        }
    }

    public V get(K key) {
        return table.get(key);
    }

    public boolean containsKey(K key) {
        return table.containsKey(key);
    }

    public ExtensibleCache<K, V> put(K key, V value) {
        table.put(key, value);
        return this;
    }

    public ExtensibleCache<K, V> remove(K key) {
        table.remove(key);
        return this;
    }

    public ExtensibleCache<K, V> clear() {
        table.clear();
        return this;
    }

    public Stream<Entry<K, V>> entryStream() {
        return table.entryStream();
    }

    @Override
    public void close() throws Exception {
        table.close();
    }
}
