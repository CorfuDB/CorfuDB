package org.corfudb.test;

import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Queues;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** The memory appender logs messages into a in-memory circular buffer which
 *  stores a limited number of entries.
 *
 * @param <E>   The event object type.
 */
@Slf4j
public class MemoryAppender<E> extends UnsynchronizedAppenderBase<E> {

    /**
     * A queue which holds encoded messages.
     */
    private Queue<byte[]> queue;

    /** An encoder which takes raw messages of type {@code E} and converts
     *  them to printable bytes.
     */
    private Encoder<E> encoder;

    /** Generate a new in-memory appender which holds a limited number of elements.
     *
     * @param maxElements   The maximum elements to hold. If exceeded, the oldest
     *                      element is evicted.
     * @param encoder       The encoder which encodes messages.
     */
    public MemoryAppender(int maxElements, @Nonnull Encoder<E> encoder) {
        queue = Queues.synchronizedQueue(EvictingQueue.create(maxElements));
        this.encoder = encoder;
    }

    /** Reset the appender, clearing any logged events. */
    public void reset() {
        queue.clear();
    }

    /** Get the events which were logged, resetting the appender. */
    public Iterable<byte[]> getEventsAndReset() {
        List<byte[]> list = new ArrayList<>();
        byte[] element = queue.poll();
        while (element != null) {
            list.add(element);
            element = queue.poll();
        }
        return list;
    }

    @Override
    protected void append(E eventObject) {
        queue.offer(encoder.encode(eventObject));
    }
}
