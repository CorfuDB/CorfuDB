package org.corfudb.runtime.view.stream;

import java.util.Spliterator;
import java.util.function.Consumer;

import static java.util.Spliterators.spliterator;


/**
 * This spliterator divides its elements into batches by a
 * fixed batch size, allowing parallel computation.
 *
 * Reference:
 * https://www.airpair.com/java/posts/parallel-processing-of-io-based-data-with-java-streams
 */
public abstract class FixedBatchSpliterator<T> implements Spliterator<T> {
    private static final int DEFAULT_BATCH = 1 << 6;

    private final int batchSize;
    private final int characteristics;
    private long est;

    protected FixedBatchSpliterator(int characteristics, int batchSize, long est) {
        this.characteristics = characteristics | SUBSIZED;
        this.batchSize = batchSize;
        this.est = est;
    }
    protected FixedBatchSpliterator(int characteristics, int batchSize) {
        this(characteristics, batchSize, Long.MAX_VALUE);
    }
    protected FixedBatchSpliterator(long est, int characteristics) {
        this(characteristics, DEFAULT_BATCH, est);
    }
    protected FixedBatchSpliterator(int characteristics) {
        this(characteristics, DEFAULT_BATCH, Long.MAX_VALUE);
    }
    protected FixedBatchSpliterator() {
        this(IMMUTABLE | ORDERED | NONNULL);
    }

    @Override
    public Spliterator<T> trySplit() {
        final HoldingConsumer<T> holder = new HoldingConsumer<>();
        if (!tryAdvance(holder)) return null;
        final Object[] a = new Object[batchSize];
        int j = 0;
        do a[j] = holder.value; while (++j < batchSize && tryAdvance(holder));
        if (est != Long.MAX_VALUE) est -= j;
        return spliterator(a, 0, j, characteristics() | SIZED);
    }

    @Override
    public long estimateSize() {
        return est;
    }

    @Override
    public int characteristics() {
        return characteristics;
    }

    private static final class HoldingConsumer<T> implements Consumer<T> {
        public Object value;

        @Override
        public void accept(T value) {
            this.value = value;
        }
    }
}
