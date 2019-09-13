package org.corfudb.benchmarks.runtime.collections.experiment.rocksdb;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.rocksdb.Options;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A persistent Map backed by RocksDb embedded database.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Builder
public class RocksDbMap<K, V> implements Map<K, V> {
    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    static {
        RocksDB.loadLibrary();
    }

    @Getter
    private RocksDB db;

    @NonNull
    private final Class<K> keyType;

    @NonNull
    private final Class<V> valueType;

    @NonNull
    @Default
    @Getter
    private final Path dbPath = Paths.get(TMP_DIR, "corfu", "rt", "persistence", "rocks_db");

    @NonNull
    @Default
    private final WriteOptions opts = new WriteOptions().setDisableWAL(true);

    @NonNull
    @Default
    private final Options options = new Options()
            .setCreateIfMissing(true)
            .setWriteBufferSize((long) 256 * (long) 1024 * (long) 1024)
            .setMaxWriteBufferNumber(4)
            .setIncreaseParallelism(8)
            //.setBloomLocality(10)

            //.setOptimizeFiltersForHits(true)

            //.setCompactionStyle(CompactionStyle.LEVEL)
            //.setCompressionType(CompressionType.NO_COMPRESSION)

            //.setMemTableConfig(new SkipListMemTableConfig())

            .useCappedPrefixExtractor(64)
            .setMemtablePrefixBloomSizeRatio(0.1)
            .setTableFormatConfig(new PlainTableConfig())

            /*
            .setTableFormatConfig(new BlockBasedTableConfig()
                    .setPartitionFilters(true)
                    .setIndexType(IndexType.kHashSearch)
                    .setPinL0FilterAndIndexBlocksInCache(true)
                    //.setCacheIndexAndFilterBlocks(true) //- huge performance degradation
                    .setCacheIndexAndFilterBlocksWithHighPriority(true)
                    .setBlockCache(new LRUCache(100_000))
                    .setFilterPolicy(new BloomFilter())
            )*/;

    public RocksDbMap<K, V> init() throws RocksDBException {
        db = RocksDB.open(options, dbPath.toString());
        return this;
    }

    public void close() {
        db.close();
    }

    @Override
    public int size() {
        int size = 0;
        try (RocksIterator iter = db.newIterator()) {
            iter.seekToFirst();
            while (iter.isValid()) {
                size++;
                iter.next();
            }
        }

        return size;
    }

    @Override
    public boolean isEmpty() {
        try (RocksIterator iter = db.newIterator()) {
            iter.seekToFirst();

            return !iter.isValid();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        try (RocksIterator iter = db.newIterator()) {
            iter.seek(serialize(key));
            return iter.isValid();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        try (RocksIterator iter = db.newIterator()) {
            iter.seekToFirst();

            while (iter.isValid()) {
                if (value.equals(ICorfuPayload.fromBuffer(iter.value(), valueType))) {
                    return true;
                }
                iter.next();
            }
            return false;
        }
    }

    @Override
    public V get(Object key) {
        try {
            byte[] value = db.get(serialize(key));
            if (value == null) {
                return null;
            }
            return ICorfuPayload.fromBuffer(value, valueType);
        } catch (RocksDBException e) {
            throw new IllegalStateException("can't put data", e);
        }
    }

    @Override
    public V put(K key, V value) {
        try {
            db.put(opts, serialize(key), serialize(value));
            return value;
        } catch (RocksDBException e) {
            throw new IllegalStateException("can't put data", e);
        }

    }

    @Override
    public V remove(Object key) {
        try {
            byte[] serializedKey = serialize(key);
            byte[] value = db.get(serializedKey);
            db.delete(serializedKey);
            return ICorfuPayload.fromBuffer(value, valueType);
        } catch (RocksDBException e) {
            throw new IllegalStateException("Error", e);
        }
    }

    public byte[] serialize(Object key) {
        ByteBuf buffer = Unpooled.buffer();
        ICorfuPayload.serialize(buffer, key);
        return buffer.array();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        map.forEach(this::put);
    }

    @Override
    public void clear() {
        try {
            db.close();
            RocksDB.destroyDB(dbPath.toString(), options);
        } catch (RocksDBException e) {
            throw new IllegalStateException("yay");
        }
    }

    @Override
    public Set<K> keySet() {
        try (RocksIterator iter = db.newIterator()) {
            iter.seekToFirst();

            Set<K> keySet = new HashSet<>();
            while (iter.isValid()) {
                keySet.add(ICorfuPayload.fromBuffer(iter.key(), keyType));
                iter.next();
            }

            return keySet;
        }
    }

    @Override
    public Collection<V> values() {
        List<V> result = new ArrayList<>();

        try (RocksIterator iter = db.newIterator()) {
            iter.seekToFirst();
            while (iter.isValid()) {
                V value = ICorfuPayload.fromBuffer(iter.value(), valueType);
                result.add(value);
                iter.next();
            }
        }

        return result;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new IllegalStateException("yay");
    }

    /**
     * https://github.com/facebook/rocksdb/blob/master/include/rocksdb/db.h
     *
     * @return database statistics
     * @throws RocksDBException db exception
     */
    public String getStats() throws RocksDBException {
        return new StringBuilder()
                .append(db.getProperty("rocksdb.dbstats"))
                .append("\n** SST **\n")
                .append(db.getProperty("rocksdb.sstables"))
                .append("\n** Levels ** \n")
                .append(db.getProperty("rocksdb.levelstats"))
                .append("\n** Immutable tables **\n")
                .append(db.getProperty("rocksdb.num-immutable-mem-table"))
                .append("\n** Size of all memtables **\n")
                .append(db.getProperty("rocksdb.size-all-mem-tables"))
                .append("\n** Estimate num keys **\n")
                .append(db.getProperty("rocksdb.estimate-num-keys"))
                .toString();
    }
}
