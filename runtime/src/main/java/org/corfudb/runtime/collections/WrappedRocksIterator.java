package org.corfudb.runtime.collections;

import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksIteratorInterface;

import java.nio.ByteBuffer;

/**
 * A wrapper class that makes a access to the RocksIterator safe: the rocks library does some checks but they
 * are enforced via assert, but assert checking is disabled on many jvms by default, hence the explicit checking.
 * <p>
 * Created by Maithem on 1/24/20.
 */
public class WrappedRocksIterator implements RocksIteratorInterface {

    private final RocksIterator iterator;

    public WrappedRocksIterator(RocksIterator iterator) {
        this.iterator = iterator;
    }

    /**
     * A helper method that is called before accessing the iterator.
     * This is necessary to make sure that the iterator hasn't been closed.
     */
    private void checkHandle() {
        if (!iterator.isOwningHandle()) {
            throw new IllegalStateException("RocksIterator has been closed");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        checkHandle();
        boolean res = iterator.isValid();
        status();
        return res;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekToFirst() {
        checkHandle();
        iterator.seekToFirst();
        status();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekToLast() {
        throw new UnsupportedOperationException("seekToLast");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seek(byte[] target) {
        throw new UnsupportedOperationException("seek");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seek(ByteBuffer target) {
        throw new UnsupportedOperationException("seek");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekForPrev(byte[] target) {
        throw new UnsupportedOperationException("seekForPrev");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekForPrev(ByteBuffer target) {
        throw new UnsupportedOperationException("seekForPrev");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void next() {
        checkHandle();
        iterator.next();
        status();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prev() {
        throw new UnsupportedOperationException("prev");
    }

    /**
     * Returns the key of the current position of the iterator.
     */
    public byte[] key() {
        checkHandle();
        byte[] res = iterator.key();
        status();
        return res;
    }

    /**
     * Returns the value of the current position of the iterator.
     */
    public byte[] value() {
        checkHandle();
        byte[] res = iterator.value();
        status();
        return res;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void status() {
        try {
            iterator.status();
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError("Iterator is in an invalid state", e);
        }
    }

    @Override
    public void refresh(){
        throw new UnsupportedOperationException("refresh");
    }

    public boolean isOpen() {
        return iterator.isOwningHandle();
    }

    /**
     * Closes the iterator. This method is idempotent.
     */
    public void close() {
        iterator.close();
    }
}
