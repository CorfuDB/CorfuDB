package org.corfudb.runtime.collections;

import io.netty.buffer.Unpooled;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

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
    final private WrappedRocksIterator wrappedRocksIterator;

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

    final private ReadOptions readOptions;

    public RocksDbEntryIterator(RocksDB rocksDB, ISerializer serializer, boolean loadValues) {
        // Start iterator at the current snapshot
        readOptions = new ReadOptions();
        readOptions.setSnapshot(null);
        this.wrappedRocksIterator = new WrappedRocksIterator(rocksDB.newIterator(readOptions));
        this.serializer = serializer;
        wrappedRocksIterator.seekToFirst();
        this.loadValues = loadValues;
    }

    public RocksDbEntryIterator(RocksDB rocksDB, ISerializer serializer) {
        this(rocksDB, serializer, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        if (next == null && wrappedRocksIterator.isOpen() && wrappedRocksIterator.isValid()) {
            // Retrieve entry if it exists and move the iterator
            K key = (K) serializer.deserialize(Unpooled.wrappedBuffer(wrappedRocksIterator.key()));
            V value = loadValues ? (V) serializer
                    .deserialize(Unpooled.wrappedBuffer(wrappedRocksIterator.value())) : null;
            next = new AbstractMap.SimpleEntry(key, value);
            wrappedRocksIterator.next();
        }

        if (next == null && wrappedRocksIterator.isOpen()) {
            // close the iterator if it has fully consumed.
            wrappedRocksIterator.close();
            readOptions.close();
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
        // Release the underlying RocksDB resources
        if (wrappedRocksIterator.isOpen()) {
            wrappedRocksIterator.close();
            readOptions.close();
        }
    }
}
