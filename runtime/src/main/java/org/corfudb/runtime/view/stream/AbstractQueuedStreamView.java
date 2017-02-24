package org.corfudb.runtime.view.stream;

import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;

import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;

/** The abstract queued stream view implements a stream backed by a read queue.
 *
 * A read queue is a priority queue where addresses can be inserted, and are
 * dequeued in ascending order. Subclasses implement the fillReadQueue()
 * function, which defines how the read queue should be filled, and the
 * read() function, which reads an entry and updates the pointers for the
 * stream view.
 *
 * The addresses in the read queue must be global addresses.
 *
 * This implementation does not handle bulk reads and depends on IStreamView's
 * implementation of remainingUpTo(), which simply calls nextUpTo() under a lock
 * until it returns null.
 *
 * Created by mwei on 1/6/17.
 */
@Slf4j
public abstract class AbstractQueuedStreamView extends
        AbstractContextStreamView<AbstractQueuedStreamView
                .QueuedStreamContext> {

    /** Create a new queued stream view.
     *
     * @param streamID  The ID of the stream
     * @param runtime   The runtime used to create this view.
     */
    public AbstractQueuedStreamView(final CorfuRuntime runtime,
                                    final UUID streamID) {
        super(runtime, streamID, QueuedStreamContext::new);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ILogData getNextEntry(QueuedStreamContext context,
                                    long maxGlobal) {
        // If we have no entries to read, fill the read queue.
        // Return if the queue is still empty.
        if (context.readQueue.isEmpty() &&
                !fillReadQueue(maxGlobal, context)) {
            return null;
        }

        // If the lowest element is greater than maxGlobal, there's nothing
        // to return.
        if (context.readQueue.first() > maxGlobal) {
            return null;
        }

        // Otherwise we remove entries one at a time from the read queue.
        // The entry may not actually be part of the stream, so we might
        // have to perform several reads.
        while (context.readQueue.size() > 0) {
            final long thisRead = context.readQueue.pollFirst();
            ILogData ld = read(thisRead);
            if (ld.containsStream(context.id)) {
                return ld;
            }
        }

        // None of the potential reads ended up being part of this
        // stream, so we return null.
        return null;
    }

    /**
     * Retrieve the data at the given address which was previously
     * inserted into the read queue.
     *
     * @param address       The address to read.
     */
    abstract protected @NonNull ILogData read(final long address);

    /**
     * Fill the read queue for the current context. This method is called
     * whenever a client requests a read, but there are no addresses left in
     * the read queue.
     *
     * This method returns true if entries were added to the read queue,
     * false otherwise.
     *
     * @param maxGlobal     The maximum global address to read to.
     * @param context       The current stream context.
     *
     * @return              True, if entries were added to the read queue,
     *                      False, otherwise.
     */
    abstract protected boolean fillReadQueue(final long maxGlobal,
                                          final QueuedStreamContext context);

    /** {@inheritDoc}
     *
     * For the queued stream context, we include just a queue of potential
     * global addresses to be read from.
     */
    @ToString
    static class QueuedStreamContext extends AbstractStreamContext {

        /**
         * A priority queue of potential addresses to be read from.
         */
        final NavigableSet<Long> readQueue
                = new TreeSet<>();

        /** Create a new stream context with the given ID and maximum address
         * to read to.
         * @param id                  The ID of the stream to read from
         * @param maxGlobalAddress    The maximum address for the context.
         */
        public QueuedStreamContext(UUID id, long maxGlobalAddress) {
            super(id, maxGlobalAddress);
        }
    }

}
