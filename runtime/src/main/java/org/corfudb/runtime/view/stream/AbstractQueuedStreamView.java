package org.corfudb.runtime.view.stream;

import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    /** Add the given address to the resolved queue of the
     * given context.
     * @param context           The context to add the address to
     * @param globalAddress     The resolved global address.
     */
    protected void addToResolvedQueue(QueuedStreamContext context,
                                      long globalAddress) {
        if (context.maxResolution < globalAddress)
        {
            context.resolvedQueue.add(globalAddress);
            context.maxResolution = globalAddress;
        }
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
                addToResolvedQueue(context, thisRead);
                return ld;
            }
        }

        // None of the potential reads ended up being part of this
        // stream, so we return null.
        return null;
    }

    /** {@inheritDoc}
     *
     * In the queued implementation, we just read all entries in the read queue
     * in parallel. If there is any entry which changes the context, we cut the
     * list off there.
     * */
    @Override
    protected List<ILogData> getNextEntries(QueuedStreamContext context, long maxGlobal,
                                            Function<ILogData, Boolean> contextCheckFn) {
        // We always have to fill to the read queue to ensure we read up to
        // max global.
        if (!fillReadQueue(maxGlobal, context)) {
            return Collections.emptyList();
        }

        // If the lowest element is greater than maxGlobal, there's nothing
        // to return.
        if (context.readQueue.first() > maxGlobal) {
            return Collections.emptyList();
        }

        // The list to store read results in
        List<ILogData> read = new ArrayList<>();

        // While we have data and haven't exceeded maxGlobal
        while (context.readQueue.size() > 0 &&
                context.readQueue.first() <= maxGlobal) {

            // Do the read, removing the request from the queue
            long readAddress = context.readQueue.pollFirst();
            ILogData data = read(readAddress);


            // Add to the read list if the entry is part of this
            // stream and contains data
            if (data.getType() == DataType.DATA &&
                    data.containsStream(context.id)) {
                addToResolvedQueue(context, readAddress);
                read.add(data);
            }

            // Update the pointer.
            context.globalPointer = readAddress;

            // If the context changed, return
            if (contextCheckFn.apply(data)) {
                return read;
            }
        }

        // Return the list of entries read.
        return read;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long find(long globalAddress, SearchDirection direction) {
        // First, check if we have resolved up to the given address
        if (getCurrentContext().maxResolution < globalAddress) {
            // If not we need to read to that position
            // to resolve all the addresses.
            remainingUpTo(globalAddress + 1);
        }

        // Now we can do the search.
        // First, check for inclusive searches.
        if (direction.isInclusive() &&
                getCurrentContext().resolvedQueue.contains(globalAddress)) {
            return globalAddress;
        }
        // Next, check all elements excluding
        // in the correct direction.
        Long result;
        if (direction.isForward()) {
            result = getCurrentContext().resolvedQueue.higher(globalAddress);
        }  else {
            result = getCurrentContext().resolvedQueue.lower(globalAddress);
        }

        // Convert the address to never read if there was no result.
        return result == null ? Address.NEVER_READ : result;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized ILogData previous() {
        // If never read, there would be no pointer to the previous entry.
        if (getCurrentContext().globalPointer == Address.NEVER_READ) {
            return null;
        }
        // If the pointer is before our min resolution, we need to resolve
        // to get the previous entry.
        if (getCurrentContext().globalPointer <
                getCurrentContext().minResolution) {
            long oldPointer = getCurrentContext().globalPointer;
            getCurrentContext().globalPointer = Address.NEVER_READ;
            remainingUpTo(getCurrentContext().minResolution);
            getCurrentContext().minResolution = Address.NEVER_READ;
            getCurrentContext().globalPointer = oldPointer;

        }
        // Otherwise, the previous entry should be resolved, so get
        // one less than the current.
        Long prevAddress = getCurrentContext()
                .resolvedQueue.lower(getCurrentContext().globalPointer);
        // Could be null, if we only read one entry
        if (prevAddress == null) {
            return null;
        }
        // Add the current pointer back into the read queue
        getCurrentContext().readQueue.add(getCurrentContext().globalPointer);
        // Update the global pointer
        getCurrentContext().globalPointer = prevAddress;
        return read(prevAddress);
    }

   /** {@inheritDoc} */
    @Override
    public synchronized ILogData current() {
        if (getCurrentContext().globalPointer == Address.NEVER_READ) {
            return null;
        }
        return read(getCurrentContext().globalPointer);
    }

    /** {@inheritDoc} */
    @Override
    public long getCurrentGlobalPosition() {
        return getCurrentContext().globalPointer;
    }


    /** {@inheritDoc}
     *
     * For the queued stream context, we include just a queue of potential
     * global addresses to be read from.
     */
    @ToString
    static class QueuedStreamContext extends AbstractStreamContext {


        /** A queue of addresses which have already been resolved. */
        final NavigableSet<Long> resolvedQueue
                = new TreeSet<>();

        /** The minimum global address which we have resolved this
         * stream to.
         */
        long minResolution = Address.NEVER_READ;

        /** The maximum global address which we have resolved this
         * stream to.
         */
        long maxResolution = Address.NEVER_READ;

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


        /** {@inheritDoc} */
        @Override
        void reset() {
            super.reset();
            readQueue.clear();
        }

        /** {@inheritDoc} */
        @Override
        void seek(long globalAddress) {
            // Update minResolution if necessary
            if (globalAddress > maxResolution) {
                minResolution = globalAddress;
            }
            // remove anything in the read queue LESS
            // than global address.
            readQueue.headSet(globalAddress).clear();
            // transfer from the resolved queue into
            // the read queue anything equal to or
            // greater than the global address
            readQueue.addAll(resolvedQueue.tailSet(globalAddress, true));
            super.seek(globalAddress);
        }
    }

}
