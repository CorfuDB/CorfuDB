package org.corfudb.runtime.collections;

import io.netty.buffer.Unpooled;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.ReadOptions;
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

    public static final boolean LOAD_VALUES = true;

    /**
     * A reference to the underlying RocksDb iterator
     */
    private final CheckedRocksIterator wrappedRocksIterator;

    /**
     * Serializer to serialize/deserialize the key/values
     */
    private final ISerializer serializer;

    /**
     * Placeholder for the current value
     */
    private Map.Entry<K, V> next;

    /**
     * Whether to load the value of a mapping
     */
    private final boolean loadValues;

    public RocksDbEntryIterator(RocksIterator rocksIterator, ISerializer serializer,
                                ReadOptions readOptions, StampedLock lock, boolean loadValues) {
        this.wrappedRocksIterator = new CheckedRocksIterator(rocksIterator, lock, readOptions);
        this.serializer = serializer;
        this.loadValues = loadValues;
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
