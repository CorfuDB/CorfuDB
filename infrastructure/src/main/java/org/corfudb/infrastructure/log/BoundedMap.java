package org.corfudb.infrastructure.log;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A BoundedMap provides an array backed map that can map long -> long where the indices are in the range of
 * (Long.MIN_VALUE, Long.MAX_VALUE - Integer.MAX_VALUE)
 */
public class BoundedMap {

    private final long offset;
    private final AtomicLongArray array;
    public static final long NOT_SET = Long.MIN_VALUE;

    public BoundedMap(long offset, int size) {
        Preconditions.checkArgument(offset >= 0);
        Preconditions.checkArgument(size > 0);

        // Validate that the sum of both arguments does not exceed Long.MAX_VALUE.
        Math.addExact(offset, size);

        this.offset = offset;
        this.array = new AtomicLongArray(size);
        for (int idx = 0; idx < array.length(); idx++) {
            this.array.set(idx, NOT_SET);
        }
    }

    private void checkRange(long num) {
        if (num < offset || num >= offset + array.length()) {
            throw new IllegalArgumentException(num + " not in [" + offset + ", " + (offset + array.length()) + ")");
        }
    }

    private void checkValue(long value) {
        Preconditions.checkArgument(NOT_SET != value, "invalid %s value", value);
    }

    private int mapIdx(long idx) {
        return Math.toIntExact(idx - offset);
    }

    public boolean set(long idx, long value) {
        checkRange(idx);
        checkValue(value);
        return array.compareAndSet(mapIdx(idx), NOT_SET, value);
    }

    public long get(long idx) {
        checkRange(idx);
        return array.get(mapIdx(idx));
    }

    public boolean contains(long idx) {
        checkRange(idx);
        return array.get(mapIdx(idx)) != NOT_SET;
    }

    public int capacity() {
        return array.length();
    }

    /**
     * Iterators are only used for initialization and therefore are not optimized. For instance, the running time
     * for iterating is not a function of the number of elements set in the index, but rather the size of the index.
     */
    public Iterator<Long> iterator() {
        return new IndexIterator(array);
    }

    public Iterable<Long> iterable() {
        final Iterator<Long> iter = iterator();
        return () -> iter;
    }

    private final class IndexIterator implements Iterator<Long> {

        private final AtomicLongArray array;
        private int idx = 0;
        private IndexIterator(AtomicLongArray array) {
            this.array = array;
            findNextIdx();
        }

        private void findNextIdx() {
            while (idx < array.length() && array.get(idx) == NOT_SET) {
                idx++;
            }
        }

        @Override
        public boolean hasNext() {
            return idx < array.length();
        }

        @Override
        public Long next() {
            long ret = array.get(idx);
            Preconditions.checkState(ret != NOT_SET);
            long retVal = idx + offset;
            idx++;
            findNextIdx();
            return retVal;
        }
    }
}
