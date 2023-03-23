package org.corfudb.runtime.collections;

import io.netty.buffer.Unpooled;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.StampedLock;

/**
 *
 * A wrapper class that provides an iterator over the RocksDb map entries. It is compatible with Java's {@link Iterator}.
 *
 * Created by Maithem on 1/21/20.
 */
@NotThreadSafe
public class RocksDbEntryIterator<K, V> implements Iterator<Map.Entry<K, V>>, AutoCloseable {

    /**
     * A reference to the underlying RocksDb iterator
     */
    final private CheckedRocksIterator wrappedRocksIterator;

    /**
     * Serializer to serialize/deserialize the key/values
     */
    final private ISerializer serializer;

    /**
     * place holder for the current value
     */
    private Map.Entry<K, V> next;

    /**
     * Whether to load the value of a mapping
     */
    private final boolean loadValues;

    public RocksDbEntryIterator(RocksDB rocksDB, ISerializer serializer, boolean loadValues) {
        this(rocksDB, serializer, new ReadOptions(), loadValues);
    }

    public RocksDbEntryIterator(RocksDB rocksDB, ISerializer serializer) {
        this(rocksDB, serializer, true);
    }

    public RocksDbEntryIterator(RocksDB rocksDB, ISerializer serializer, ReadOptions readOptions, boolean loadValues) {
        throw new UnsupportedOperationException();
    }

    public RocksDbEntryIterator(RocksIterator rocksIterator, ISerializer serializer,
                                ReadOptions readOptions, StampedLock lock) {
        this.wrappedRocksIterator = new CheckedRocksIterator(rocksIterator, lock, readOptions);
        this.serializer = serializer;
        this.loadValues = true;
        wrappedRocksIterator.seekToFirst();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        if (next == null && wrappedRocksIterator.isOpen() && wrappedRocksIterator.isValid()) {
            // Retrieve entry if it exists and move the iterator
            K key = (K) serializer.deserialize(Unpooled.wrappedBuffer(wrappedRocksIterator.key()), null);
            V value = loadValues ? (V) serializer
                    .deserialize(Unpooled.wrappedBuffer(wrappedRocksIterator.value()), null) : null;
            next = new AbstractMap.SimpleEntry(key, value);
            wrappedRocksIterator.next();
        }

        if (next == null && wrappedRocksIterator.isOpen()) {
            // close the iterator if it has fully consumed.
            wrappedRocksIterator.close();
        }

        return next != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map.Entry<K, V> next() {
        if (hasNext()) {
            Map.Entry<K, V> res = next;
            next = null;
            return res;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // Release the underlying RocksDB resources.
        if (wrappedRocksIterator.isOpen()) {
            wrappedRocksIterator.close();
        }
    }

    public void invalidateIterator() {
        wrappedRocksIterator.invalidateIterator();
    }
}
