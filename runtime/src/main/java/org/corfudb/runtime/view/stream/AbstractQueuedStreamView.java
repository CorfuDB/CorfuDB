package org.corfudb.runtime.view.stream;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AppendException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.runtime.view.StreamOptions;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;


/** The abstract queued stream view implements a stream backed by a read queue.
 *
 * <p>A read queue is a priority queue where addresses can be inserted, and are
 * dequeued in ascending order. Subclasses implement the fillReadQueue()
 * function, which defines how the read queue should be filled, and the
 * read() function, which reads an entry and updates the pointers for the
 * stream view.
 *
 * <p>The addresses in the read queue must be global addresses.
 *
 * <p>This implementation does not handle bulk reads and depends on IStreamView's
 * implementation of remainingUpTo(), which simply calls nextUpTo() under a lock
 * until it returns null.
 *
 * <p>Created by mwei on 1/6/17.
 */
@Slf4j
public abstract class AbstractQueuedStreamView extends
        AbstractContextStreamView<AbstractQueuedStreamView
                .QueuedStreamContext> {
    @Getter
    private final ReadOptions readOptions;

    /** Create a new queued stream view.
     *
     * @param streamId  The ID of the stream
     * @param runtime   The runtime used to create this view.
     */
    public AbstractQueuedStreamView(final CorfuRuntime runtime,
                                    final UUID streamId,
                                    StreamOptions streamOptions) {
        super(runtime, streamId, QueuedStreamContext::new);
        this.readOptions = ReadOptions.builder()
                .clientCacheable(streamOptions.isCacheEntries())
                .build();
    }

    /** Add the given address to the resolved queue of the
     * given context.
     * @param context           The context to add the address to
     * @param globalAddress     The resolved global address.
     */
    protected void addToResolvedQueue(QueuedStreamContext context,
                                      long globalAddress) {
        context.resolvedQueue.add(globalAddress);

        if (context.maxResolution < globalAddress) {
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
        if (context.readQueue.isEmpty() && !fillReadQueue(maxGlobal, context)) {
            return null;
        }

        // If the lowest DATA element is greater than maxGlobal, there's nothing
        // to return.
        if (context.readQueue.first() > maxGlobal) {
            return null;
        }

        return removeFromQueue(context.readQueue, maxGlobal);
    }

    /**
     * Remove next entry from the queue.
     *
     * @param queue queue of entries.
     * @param snapshot snapshot timestamp.
     * @return next available entry. or null if there are no more entries
     *         or remaining entries are not part of this stream.
     */
    protected abstract ILogData removeFromQueue(NavigableSet<Long> queue, long snapshot);

    /**
     * {@inheritDoc}
     */
    @Override
    public void gc(long trimMark) {
        // GC stream only if the pointer is ahead from the trim mark,
        if (getCurrentContext().getGlobalPointer() >= trimMark) {
            log.debug("gc[{}]: start GC on stream {} for trim mark {}", this, this.getId(),
                    trimMark);

            // Remove all the entries that are strictly less than
            // the trim mark (compaction mark).
            getCurrentContext().readQueue.headSet(trimMark).clear();
            getCurrentContext().resolvedQueue.headSet(trimMark).clear();

            if (!getCurrentContext().resolvedQueue.isEmpty()) {
                getCurrentContext().minResolution = getCurrentContext()
                        .resolvedQueue.first();
            }
        } else {
            log.debug("gc[{}]: GC not performed on stream {}. Global pointer {} is below trim mark {}",
                    this, this.getId(), getCurrentContext().getGlobalPointer(), trimMark);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>We loop forever trying to
     * write, and automatically retrying if we get overwritten (hole filled).
     */
    @Override
    public long append(Object object,
                       Function<TokenResponse, Boolean> acquisitionCallback,
                       Function<TokenResponse, Boolean> deacquisitionCallback) {
        final LogData ld = new LogData(DataType.DATA, object, runtime.getParameters().getCodecType());

        // Validate if the  size of the log data is under max write size.
        ld.checkMaxWriteSize(runtime.getParameters().getMaxWriteSize());

        // First, we get a token from the sequencer.
        TokenResponse tokenResponse = runtime.getSequencerView()
                .next(id);

        // We loop forever until we are interrupted, since we may have to
        // acquire an address several times until we are successful.
        for (int x = 0; x < runtime.getParameters().getWriteRetry(); x++) {
            // Next, we call the acquisitionCallback, if present, informing
            // the client of the token that we acquired.
            if (acquisitionCallback != null) {
                if (!acquisitionCallback.apply(tokenResponse)) {
                    // The client did not like our token, so we end here.
                    // We'll leave the hole to be filled by the client or
                    // someone else.
                    log.debug("Acquisition rejected token={}", tokenResponse);
                    return -1L;
                }
            }

            // Now, we do the actual write. We could get an overwrite
            // exception here - any other exception we should pass up
            // to the client.
            try {
                runtime.getAddressSpaceView().write(tokenResponse, ld);
                // The write completed successfully, so we return this
                // address to the client.
                return tokenResponse.getToken().getSequence();
            } catch (OverwriteException oe) {
                log.trace("Overwrite occurred at {}", tokenResponse);
                // We got overwritten, so we call the deacquisition callback
                // to inform the client we didn't get the address.
                if (deacquisitionCallback != null) {
                    if (!deacquisitionCallback.apply(tokenResponse)) {
                        log.debug("Deacquisition requested abort");
                        return -1L;
                    }
                }
                // Request a new token, informing the sequencer we were
                // overwritten.
                tokenResponse = runtime.getSequencerView().next(id);
            } catch (StaleTokenException te) {
                log.trace("Token grew stale occurred at {}", tokenResponse);
                if (deacquisitionCallback != null && !deacquisitionCallback.apply(tokenResponse)) {
                    log.debug("Deacquisition requested abort");
                    return -1L;
                }
                // Request a new token, informing the sequencer we were
                // overwritten.
                tokenResponse = runtime.getSequencerView().next(id);
            }
        }

        log.error("append[{}]: failed after {} retries, write size {} bytes",
                tokenResponse.getSequence(),
                runtime.getParameters().getWriteRetry(),
                ILogData.getSerializedSize(object, runtime.getParameters().getCodecType()));
        throw new AppendException();
    }

    /**
     * Reads data from an address in the address space.
     *
     * It will give the writer a chance to complete based on the time
     * when the reads of which this individual read is a step started.
     * If the reads have been going on for longer than the grace period
     * given for a writer to complete a write, the subsequent individual
     * read calls will immediately fill the hole on absence of data at
     * the given address.
     *
     * @param address       address to read.
     * @param readStartTime start time of the range of reads.
     * @param snapshot      snapshot of this read.
     * @return log data at the address.
     */
    @Deprecated
    protected ILogData forceRead(final long address, long readStartTime, long snapshot) {
        // check compaction mark before access address space view. This check prevents unnecessary address space view
        // read.
        compactionMarkCheck(snapshot);
        ILogData logData;

        if (System.currentTimeMillis() - readStartTime <
                runtime.getParameters().getHoleFillTimeout().toMillis()) {
            logData = runtime.getAddressSpaceView().read(address, readOptions);
        } else {
            ReadOptions options = readOptions.toBuilder()
                    .waitForHole(false)
                    .build();
            logData = runtime.getAddressSpaceView().read(address, options);
        }

        // check compaction mark after address space view read. The compaction mark may be updated after address
        // space view read.
        compactionMarkCheck(snapshot);
        return logData;
    }

    @NonNull
    protected List<ILogData> readAll(@NonNull List<Long> addresses, long snapshot) {
        // check compaction mark before access address space view. This check prevents unnecessary address space view
        // read.
        compactionMarkCheck(snapshot);

        Map<Long, ILogData> dataMap =
                runtime.getAddressSpaceView().read(addresses, readOptions);

        // check compaction mark after address space view read. The compaction mark may be updated after address
        // space view read.
        compactionMarkCheck(snapshot);


        // If trimmed exceptions are ignored, the data retrieved by the read API might not correspond
        // to all requested addresses, for this reason we must filter out data entries not included (null).
        // Also, we need to preserve ordering for checkpoint logic.
        return  addresses.stream().map(dataMap::get)
                .filter(data -> data != null)
                .collect(Collectors.toList());
    }

    /** {@inheritDoc}
     *
     * <p>In the queued implementation, we just read all entries in the read queue
     * in parallel. If there is any entry which changes the context, we cut the
     * list off there.
     * */
    @Override
    protected List<ILogData> getNextEntries(QueuedStreamContext context, long maxGlobal,
                                            Function<ILogData, Boolean> contextCheckFn) {
        NavigableSet<Long> readSet = new TreeSet<>();

        // Scan backward in the stream to find interesting
        // log records less than or equal to maxGlobal.
        // Boolean includes both CHECKPOINT & DATA entries.
        boolean readQueueIsEmpty = !fillReadQueue(maxGlobal, context);

        // We always have to fill to the read queue to ensure we read up to
        // max global.
        if (readQueueIsEmpty) {
            return Collections.emptyList();
        }

        if (!context.readQueue.isEmpty() && context.readQueue.first() > maxGlobal) {
            // If the lowest element is greater than maxGlobal, there's nothing
            // more to return: readSet is ok as-is.
        } else {
            // Select everything in the read queue between
            // the start and maxGlobal
            readSet.addAll(context.readQueue.headSet(maxGlobal, true));
        }

        List<Long> toRead = readSet.stream()
                .collect(Collectors.toList());

        // The list to store read results in
        List<ILogData> readFrom = readAll(toRead, maxGlobal).stream()
                .filter(x -> x.getType() == DataType.DATA)
                .filter(x -> x.containsStream(context.id))
                .collect(Collectors.toList());

        // If any entries change the context,
        // don't return anything greater than
        // that entry
        Optional<ILogData> contextEntry = readFrom.stream()
                .filter(contextCheckFn::apply).findFirst();
        if (contextEntry.isPresent()) {
            log.trace("getNextEntries[{}] context switch @ {}", this,
                    contextEntry.get().getGlobalAddress());
            int idx = readFrom.indexOf(contextEntry.get());
            readFrom = readFrom.subList(0, idx + 1);
            // NOTE: readSet's clear() changed underlying context.readQueue
            readSet.headSet(contextEntry.get().getGlobalAddress(), true).clear();
        } else {
            // Clear the entries which were read
            context.readQueue.headSet(maxGlobal, true).clear();
        }

        // Transfer the addresses of the read entries to the resolved queue
        readFrom.stream()
                .forEach(x -> addToResolvedQueue(context, x.getGlobalAddress()));

        // Update the global pointer
        if (readFrom.size() > 0) {
            context.setGlobalPointer(readFrom.get(readFrom.size() - 1)
                    .getGlobalAddress());
        }

        return readFrom;
    }

    /**
     * Fill the read queue for the current context. This method is called
     * whenever a client requests a read, but there are no addresses left in
     * the read queue.
     *
     * <p>This method returns true if entries were added to the read queue,
     * false otherwise.
     *
     * @param maxGlobal     The maximum global address to read to.
     * @param context       The current stream context.
     *
     * @return              True, if entries were added to the read queue,
     *                      False, otherwise.
     *
     * {@inheritDoc}
     */
    protected boolean fillReadQueue(final long maxGlobal,
                                    final QueuedStreamContext context) {
        log.trace("Fill_Read_Queue[{}] Max: {}, Current: {}, Resolved: {} - {}", this,
                maxGlobal, context.getGlobalPointer(), context.maxResolution, context.minResolution);
        log.trace("Fill_Read_Queue[{}]: addresses in this stream Resolved queue {}" +
                        " - ReadQueue {}", this,
                context.resolvedQueue, context.readQueue);

        // The maximum address we will fill to.
        final long maxAddress =
                Long.min(maxGlobal, context.maxGlobalAddress);

        // If we already reached maxAddress ,
        // we return since there is nothing left to do.
        if (context.getGlobalPointer() >= maxAddress) {
            return false;
        }

        // If everything is available in the resolved
        // queue, use it
        if (context.maxResolution >= maxAddress
                && context.minResolution < context.getGlobalPointer()) {
            return fillFromResolved(maxGlobal, context);
        }

        Long latestTokenValue = null;

        // If the max has been resolved, use it.
        if (maxGlobal != Address.MAX) {
            latestTokenValue = context.resolvedQueue.ceiling(maxGlobal);
        }

        // If we don't have a larger token in resolved, or the request was for
        // a linearized read, fetch the token from the sequencer.
        if (latestTokenValue == null || maxGlobal == Address.MAX) {
            // The stream tail might be ahead of maxGlobal (our max timestamp to resolve up to)
            // We could limit it to the min between these two (maxGlobal and tail), but that could
            // lead to reading an address (maxGlobal) that does not belong to our stream and attempt
            // to deserialize it, or furthermore abort due to a trim on an address that does not even
            // belong to our stream.
            // For these reasons, we will keep this as the high boundary and prune
            // our discovered space of addresses up to maxGlobal.
            latestTokenValue = runtime.getSequencerView().query(context.id);
            log.trace("Fill_Read_Queue[{}] Fetched tail {} from sequencer", this, latestTokenValue);
        }

        // If there is no information on the tail of the stream, return,
        // there is nothing to do
        if (Address.nonAddress(latestTokenValue)) {
            // sanity check:
            // currently, the only possible non-address return value for a token-query
            // is Address.NON_EXIST
            if (latestTokenValue != Address.NON_EXIST) {
                log.warn("TOKEN[{}] unexpected return value", latestTokenValue);
            }
            return false;
        }

        // If everything is available in the resolved
        // queue, use it
        if (context.maxResolution >= latestTokenValue
                && context.minResolution < context.getGlobalPointer()) {
            return fillFromResolved(latestTokenValue, context);
        }

        long stopAddress = context.globalPointer;

        // We check if we can fill partially from the resolved queue
        // This is a requirement for the getStreamAddressMaps as it considers the content of the resolved queue
        // to decide if needs to fetch the address maps or not.
        if (context.globalPointer < context.maxResolution) {
            if (fillFromResolved(context.maxResolution, context)) {
                stopAddress = context.maxResolution;
                log.trace("fillReadQueue[{}]: current pointer: {}, resolved up to: {}, readQueue: {}, " +
                                "new stop address: {}", this, context.globalPointer,
                        context.maxResolution, context.readQueue, stopAddress);
            }
        }

        // Now we fetch the address map for this stream from the sequencer in a single call, i.e.,
        // addresses of this stream in the range (stopAddress, startAddress==latestTokenValue]
        discoverAddressSpace(context.id, context.readQueue,
                latestTokenValue,
                stopAddress,
                d -> true,  maxGlobal);

        return !context.readQueue.isEmpty();
    }

    /**
     * Defines the strategy to discover addresses belonging to this stream.
     *
     * We currently support two mechanisms:
     *      - Following backpointers (@see org.corfudb.runtime.view.stream.BackpointerStreamView)
     *      - Requesting the sequencer for the complete address map of a stream.
     *      (@see org.corfudb.runtime.view.stream.AddressMapStreamView)
     *
     * @param streamId stream unique identifier.
     * @param queue queue to fill up.
     * @param startAddress read start address (inclusive)
     * @param stopAddress read stop address (exclusive)
     * @param filter filter to apply to data
     * @param maxGlobal max address to resolve discovery.
     *
     * @return true if addresses were discovered, false, otherwise.
     */
    protected abstract boolean discoverAddressSpace(final UUID streamId,
                                                    final NavigableSet<Long> queue,
                                                    final long startAddress,
                                                    final long stopAddress,
                                                    final Function<ILogData, Boolean> filter,
                                                    final long maxGlobal);

    protected boolean fillFromResolved(final long maxGlobal,
                                       final QueuedStreamContext context) {
        // There's nothing to read if we're already past maxGlobal.
        if (maxGlobal < context.getGlobalPointer()) {
            return false;
        }
        // Get the subset of the resolved queue, which starts at
        // globalPointer and ends at maxAddress inclusive.
        NavigableSet<Long> resolvedSet =
                context.resolvedQueue.subSet(context.getGlobalPointer(),
                        false, maxGlobal, true);

        // Put those elements in the read queue
        context.readQueue.addAll(resolvedSet);

        return !context.readQueue.isEmpty();
    }

    /**
     *  {@inheritDoc}
     *
     **/
    protected ILogData read(long address, long snapshot) {
        // checks compaction mark to prevent unnecessary address space view read
        compactionMarkCheck(snapshot);

        ILogData logData = runtime.getAddressSpaceView().read(address, readOptions);

        // checks compaction mark again after address space view read which may updates compaction mark.
        compactionMarkCheck(snapshot);
        return logData;
    }

    /**
     *  This method reads a batch of addresses if 'nextRead' is not found in the cache.
     *  In the case of a cache miss, it piggybacks on the read for 'nextRead'.
     *
     *  If 'nextRead' is present in the cache, it directly returns this data.
     *
     * @param nextRead current address of interest
     * @param addresses batch of addresses to read (bring into the cache) in case there is a cache miss (includes
     *                  nextRead)
     * @param snapshot snapshot timestamp.
     * @return data for current 'address' of interest.
     */
    protected @Nonnull ILogData read(long nextRead, @Nonnull final NavigableSet<Long> addresses, long snapshot) {
        // checks compaction mark to prevent unnecessary address space view read
        compactionMarkCheck(snapshot);

        ILogData logData = runtime.getAddressSpaceView().read(nextRead, addresses, readOptions);

        // checks compaction mark again after address space view read which may updates compaction mark.
        compactionMarkCheck(snapshot);

        return logData;
    }

    /**
     * {@inheritDoc}
     *
     * <p> We indicate we may have entries available
     * if the read queue contains entries to read -or-
     * if the next token is greater than our log pointer.
     */
    @Override
    public boolean getHasNext(QueuedStreamContext context) {
        return  !context.readQueue.isEmpty()
                || runtime.getSequencerView().query(context.id)
                > context.getGlobalPointer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {}

    /**
     * {@inheritDoc}
     * */
    @Override
    public synchronized ILogData previous() {
        final QueuedStreamContext context = getCurrentContext();
        final long oldPointer = context.getGlobalPointer();

        log.trace("previous[{}]: max={} min={}", this,
                context.maxResolution,
                context.minResolution);

        // If never read, there would be no pointer to the previous entry.
        if (context.getGlobalPointer() == Address.NEVER_READ) {
            return null;
        }

        // check compaction mark to prevent unnecessary stream view resolution
        compactionMarkCheck(oldPointer - 1L);

        // Otherwise, the previous entry should be resolved, so get
        // one less than the current.
        Long prevAddress = context
                .resolvedQueue.lower(context.getGlobalPointer());
        // If the pointer is before our min resolution, we need to resolve
        // to get the correct previous entry.
        if (prevAddress == null && Address.isAddress(context.minResolution)
                || prevAddress != null && prevAddress <= context.minResolution) {
            context.setGlobalPointer(prevAddress == null ? Address.NEVER_READ :
                    prevAddress - 1L);

            remainingUpTo(context.minResolution);
            context.minResolution = Address.NON_ADDRESS;
            context.setGlobalPointer(oldPointer);
            prevAddress = context
                    .resolvedQueue.lower(context.getGlobalPointer());
            log.trace("previous[{}]: updated resolved queue {}", this, context.resolvedQueue);
        }

        // Clear the read queue, it may no longer be valid
        context.readQueue.clear();

        if (prevAddress != null) {
            // check compaction mark
            compactionMarkCheck(prevAddress);

            log.trace("previous[{}]: updated read queue {}", this, context.readQueue);

            context.setGlobalPointer(prevAddress);

            // The ILogData returned by previous() would be used for rollback.
            return read(prevAddress, prevAddress);
        }

        // The stream hasn't been checkpointed and we need to
        // move the stream pointer to an address before the first
        // entry
        log.trace("previous[{}]: reached the beginning of the stream resetting" +
                " the stream pointer to {}", this, Address.NON_ADDRESS);
        context.setGlobalPointer(Address.NON_ADDRESS);
        return null;
    }


    /**
    * {@inheritDoc}
    * */
    @Override
    public synchronized ILogData current() {
        final QueuedStreamContext context = getCurrentContext();

        if (Address.nonAddress(context.getGlobalPointer())) {
            return null;
        }

        // The ILogData returned by current() would be used for rollback.
        return read(context.getGlobalPointer(), context.getGlobalPointer());
    }

    /**
     * {@inheritDoc}
     * */
    @Override
    public long getCurrentGlobalPosition() {
        return getCurrentContext().getGlobalPointer();
    }

    @VisibleForTesting
    AbstractQueuedStreamView.QueuedStreamContext getContext() {
        return this.baseContext;
    }

    /** {@inheritDoc}
     *
     * <p>For the queued stream context, we include just a queue of potential
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
        long minResolution = Address.NON_ADDRESS;

        /** The maximum global address which we have resolved this
         * stream to.
         */
        long maxResolution = Address.NON_ADDRESS;

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

        /**
         * {@inheritDoc}
         * */
        @Override
        void reset() {
            super.reset();
            readQueue.clear();
            resolvedQueue.clear();
            minResolution = Address.NON_ADDRESS;
            maxResolution = Address.NON_ADDRESS;
        }

        /**
         * {@inheritDoc}
         * */
        @Override
        synchronized void seek(long globalAddress) {
            if (Address.nonAddress(globalAddress)) {
                throw new IllegalArgumentException("globalAddress must"
                        + " be >= Address.maxNonAddress()");
            }
            log.trace("Seek[{}]({}), min={} max={}", this,  globalAddress,
                    minResolution, maxResolution);
            // Update minResolution if necessary
            if (globalAddress >= maxResolution) {
                log.trace("set min res to {}" , globalAddress);
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
