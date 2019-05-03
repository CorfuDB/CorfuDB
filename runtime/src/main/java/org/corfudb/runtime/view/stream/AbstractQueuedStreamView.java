package org.corfudb.runtime.view.stream;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AppendException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.QueueAddressManager;
import org.corfudb.util.Utils;


/** The abstract queued stream view implements a stream backed by a read queue.
 *
 * <p>A read queue is a priority queue where addresses can be inserted, and are
 * dequeued in ascending order. Subclasses implement the discoverAddressSpace()
 * function, which defines how the read queue should be filled or in other words
 * how the space of addresses of a stream is discovered. Subclasses also implement
 * the removeFromQueue() which defines the strategy to remove from the read queue.
 *
 * <p>Created by mwei on 1/6/17.
 * <p>Refactored by annym on 5/3/19
 */
@Slf4j
public abstract class AbstractQueuedStreamView implements IStreamView {

    protected enum BackpointerOp {
        INCLUDE,        /** Include this address. */
        EXCLUDE,        /** Exclude this address. */
        INCLUDE_STOP    /** Stop, but also include this address. */
    }

    /**
     * ID of the stream.
     */
    @Getter
    protected final UUID id;

    /**
     * Runtime the stream view was created with.
     */
    protected final CorfuRuntime runtime;

    /**
     * Manages the read queue for this stream view.
     */
    protected final QueueAddressManager queueAddressManager;

    /**
     * Counts the number of updates to this stream.
     */
    protected int updateCounter = 0;

    /** Create a new stream view.
     *
     * @param runtime   The runtime used to create this view.
     * @param streamId  The ID of the stream
     */
    public AbstractQueuedStreamView(final CorfuRuntime runtime,
                                    final UUID streamId) {
        this.runtime = runtime;
        this.id = streamId;
        this.queueAddressManager = new QueueAddressManager(streamId);
    }

    /**
     * Remove next entry from the queue.
     *
     * @param queue queue of entries.
     * @return next available entry. or null if there are no more entries
     *         or the remaining entries are not part of this stream.
     */
    protected abstract ILogData removeFromQueue(NavigableSet<Long> queue);

    /**
     * Defines the strategy to discover addresses belonging to this stream.
     *
     * We currently support two mechanisms:
     *      - Following backpointers (@see org.corfudb.runtime.view.stream.BackpointerStreamView)
     *      - Requesting the sequencer for the complete address map of a stream.
     *      (@see org.corfudb.runtime.view.stream.AddressMapStreamView)
     *
     * @param streamId stream unique identifier.
     * @param startAddress read start address (inclusive)
     * @param stopAddress read stop address (exclusive)
     * @param checkpointFilter filter to apply to data
     * @param checkpoint true if checkpoint discovery, false otherwise.
     * @param maxGlobal max address to resolve discovery.
     *
     * @return true if addresses were discovered, false, otherwise.
     */
    protected abstract boolean discoverAddressSpace(final UUID streamId,
                                                    final long startAddress,
                                                    final long stopAddress,
                                                    final Function<ILogData, BackpointerOp> checkpointFilter,
                                                    final boolean checkpoint,
                                                    final long maxGlobal);

    /**
     * {@inheritDoc}
     *
     * <p>We loop for a predefined number of retries trying to
     * write, and automatically retrying if we get overwritten (hole filled).
     */
    @Override
    public long append(Object object,
                       Function<TokenResponse, Boolean> acquisitionCallback,
                       Function<TokenResponse, Boolean> deacquisitionCallback) {
        final LogData ld = new LogData(DataType.DATA, object);
        // Validate if the size of the log data is under max write size.
        ld.checkMaxWriteSize(runtime.getParameters().getMaxWriteSize());

        // First, we get a token from the sequencer.
        TokenResponse tokenResponse = runtime.getSequencerView().next(id);

        // We loop max for writeRetry times, we may have to
        // acquire an address several times until we are successful.
        for (int x = 0; x < runtime.getParameters().getWriteRetry(); x++) {
            // Next, we call the acquisitionCallback, if present, informing
            // the client of the token that we acquired.
            if (acquisitionCallback != null && !acquisitionCallback.apply(tokenResponse)) {
                // The client did not like our token, so we end here.
                // We'll leave the hole to be filled by the client or
                // someone else.
                log.debug("Acquisition rejected token={}", tokenResponse);
                return Address.NON_ADDRESS;
            }

            // Now, we do the actual write. We could get an overwrite or stale token
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
                if (requestAbort(deacquisitionCallback, tokenResponse)) {
                    log.debug("Deacquisition requested abort");
                    return Address.NON_ADDRESS;
                }
                // Request a new token
                tokenResponse = runtime.getSequencerView().next(id);
            } catch (StaleTokenException te) {
                log.trace("Token grew stale occurred at {}", tokenResponse);
                if (requestAbort(deacquisitionCallback, tokenResponse)) {
                    log.debug("Deacquisition requested abort");
                    return Address.NON_ADDRESS;
                }
                // Request a new token
                tokenResponse = runtime.getSequencerView().next(id);
            }
        }

        log.error("append[{}]: failed after {} retries, write size {} bytes",
                tokenResponse.getSequence(),
                runtime.getParameters().getWriteRetry(),
                ILogData.getSerializedSize(object));
        throw new AppendException();
    }

    private boolean requestAbort(Function<TokenResponse, Boolean> deacquisitionCallback,
                                 TokenResponse tokenResponse) {
        return deacquisitionCallback != null
                && !deacquisitionCallback.apply(tokenResponse);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized ILogData current() {
        if (Address.nonAddress(queueAddressManager.getGlobalPointer())) {
            return null;
        }
        return read(queueAddressManager.getGlobalPointer());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized ILogData previous() {
        final long currentPointer = queueAddressManager.getGlobalPointer();

        log.trace("previous[{}]: max={} min={}", this,
                queueAddressManager.getMaxResolution(),
                queueAddressManager.getMinResolution());

        // If never read, there would be no pointer to the previous entry.
        if (currentPointer == Address.NEVER_READ) {
            return null;
        }

        // If we're attempting to go prior to most recent checkpoint, we
        // throw a TrimmedException.
        if (currentPointer - 1 < queueAddressManager.getCheckpointSuccessStartAddr()) {
            throw new TrimmedException();
        }

        // Otherwise, the previous entry should be resolved, so get
        // one less than the current.
        Long prevAddress = queueAddressManager
                .resolvedQueue.lower(queueAddressManager.getGlobalPointer());
        // If the pointer is before our min resolution, we need to resolve
        // to get the correct previous entry.
        if (prevAddress == null && Address.isAddress(queueAddressManager.getMinResolution())
                || prevAddress != null && prevAddress <= queueAddressManager.getMinResolution()) {
            queueAddressManager.setGlobalPointerCheckGCTrimMark(prevAddress == null ? Address.NEVER_READ :
                    prevAddress - 1L);

            remainingUpTo(queueAddressManager.getMinResolution());
            queueAddressManager.setMinResolution(Address.NON_ADDRESS);
            queueAddressManager.setGlobalPointerCheckGCTrimMark(currentPointer);
            prevAddress = queueAddressManager
                    .resolvedQueue.lower(queueAddressManager.getGlobalPointer());
            log.trace("previous[{}]: updated resolved queue {}", this, queueAddressManager.resolvedQueue);
        }

        // Clear the read queue, it may no longer be valid
        queueAddressManager.readQueue.clear();

        if (prevAddress != null) {
            log.trace("previous[{}]: updated read queue {}", this, queueAddressManager.readQueue);
            queueAddressManager.setGlobalPointerCheckGCTrimMark(prevAddress);
            return read(prevAddress);
        }

        if (queueAddressManager.getCheckpointSuccessId() == null) {
            // The stream hasn't been checkpointed and we need to
            // move the stream pointer to an address before the first
            // entry
            log.trace("previous[{}]: reached the beginning of the stream resetting" +
                    " the stream pointer to {}", this, Address.NON_ADDRESS);
            queueAddressManager.setGlobalPointerCheckGCTrimMark(Address.NON_ADDRESS);
            return null;
        }

        if (queueAddressManager.resolvedQueue.first() == queueAddressManager.getGlobalPointer()) {
            log.trace("previous[{}]: reached the beginning of the stream resetting" +
                    " the stream pointer to checkpoint version {}", this, queueAddressManager.getCheckpointSuccessStartAddr());
            // Note: this is a checkpoint, we do not need to verify it is before the trim mark, it actually should be
            // cause this is the last address of the trimmed range.
            queueAddressManager.setGlobalPointer(queueAddressManager.getCheckpointSuccessStartAddr());
            return null;
        }

        throw new IllegalStateException("The stream pointer seems to be corrupted!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final synchronized ILogData nextUpTo(final long maxGlobal) {
        // Don't do anything if we've already exceeded the global pointer.
        if (queueAddressManager.getGlobalPointer() > maxGlobal) {
            return null;
        }

        // Get the next entry from the underlying implementation.
        final ILogData entry = getNextEntry(maxGlobal);

        // Update the global pointer, if it is non-checkpoint data.
        if (entry != null && entry.getType() == DataType.DATA && !entry.hasCheckpointMetadata()) {
            // Note: here we only set the global pointer and do not validate its position with respect to the trim mark,
            // as the pointer is expected to be moving step by step (for instance when syncing a stream up to maxGlobal)
            // The validation is deferred to these methods which call it in advance based on the expected final position
            // of the pointer.
            queueAddressManager.setGlobalPointer(entry.getGlobalAddress());
        }

        // Return the entry.
        return entry;
    }

    /** Retrieve the next entry in the stream
     *
     * @param maxGlobal     The maximum global address to read to.
     * @return              Next ILogData for this context
     */
    protected ILogData getNextEntry(long maxGlobal) {
        // If we have no entries to read, fill the read queue.
        // Return if the queue is still empty.
        if (queueAddressManager.isQueueEmpty() && !fillReadQueue(maxGlobal)) {
            return null;
        }

        // If maxGlobal is before the checkpoint position, throw a
        // trimmed exception
        if (maxGlobal < queueAddressManager.getCheckpointSuccessStartAddr()) {
            throw new TrimmedException();
        }

        // Determine the queue to read from (checkpoint or regular stream queue)
        NavigableSet<Long> currentReadQueue = queueAddressManager.getReadQueue(maxGlobal);

        if (currentReadQueue == null) {
            return null;
        }

        return removeFromQueue(currentReadQueue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final synchronized List<ILogData> remainingUpTo(long maxGlobal) {

        final List<ILogData> entries = getNextEntries(maxGlobal);

        // Nothing read, nothing to process.
        if (entries.isEmpty()) {
            // We've resolved up to maxGlobal, so remember it. (if it wasn't max)
            if (maxGlobal != Address.MAX) {
                // Set Global Pointer and check that it is not pointing to an address in the trimmed space.
                queueAddressManager.setGlobalPointerCheckGCTrimMark(maxGlobal);
            }
            return entries;
        }

        // Otherwise update the pointer
        if (maxGlobal != Address.MAX) {
            // Set Global Pointer and check that it is not pointing to an address in the trimmed space.
            queueAddressManager.setGlobalPointerCheckGCTrimMark(maxGlobal);
        } else {
            // Update pointer from log data and then validate final position of the pointer against GC trim mark.
            updatePointer(entries.get(entries.size() - 1));
            queueAddressManager.validateGlobalPointerPositionForGC(getCurrentGlobalPosition());
        }

        // And return the entries.
        return entries;
    }

    /** Update the global pointer, given an entry.
     *
     * @param data  The entry to use to update the pointer.
     */
    private void updatePointer(final ILogData data) {
        // Update the global pointer, if it is non-checkpoint data.
        if (data.getType() == DataType.DATA && !data.hasCheckpointMetadata()) {
            // Note: here we only set the global pointer and do not validate its position with respect to the trim mark,
            // as the pointer is expected to be moving step by step (for instance when syncing a stream up to maxGlobal)
            // The validation is deferred to these methods which call it in advance based on the expected final position
            // of the pointer.
            queueAddressManager.setGlobalPointer(data.getGlobalAddress());
        }
    }

    /** Retrieve the next entries in the stream.
     *
     * <p>This function is designed to read all entries in the read queue
     *    in parallel.
     *
     * <p>The default implementation simply calls getNextEntry.
     *
     * @param maxGlobal         The maximum global address to read to.
     *
     * @return                  A list of the next entries for this context
     */
    protected List<ILogData> getNextEntries(long maxGlobal) {
        // Scan backward in the stream to find interesting
        // log records less than or equal to maxGlobal.
        // Boolean includes both CHECKPOINT & DATA entries.
        boolean readQueueIsEmpty = !fillReadQueue(maxGlobal);

        // If maxGlobal is before the checkpoint position, throw a
        // trimmed exception
        if (maxGlobal < queueAddressManager.getCheckpointSuccessStartAddr()) {
            throw new TrimmedException();
        }

        // We always have to fill to the read queue to ensure we read up to
        // max global.
        if (readQueueIsEmpty) {
            return Collections.emptyList();
        }

        List<Long> readList = queueAddressManager.getAllReads(maxGlobal);

        // The list to store read results in
        List<ILogData> readResults = readAll(readList).stream()
                .filter(x -> x.getType() == DataType.DATA)
                .filter(x -> x.containsStream(id))
                .collect(Collectors.toList());

        // Clear the entries which were read
        queueAddressManager.clearReadQueue(maxGlobal);

        // Transfer the addresses of the read entries to the resolved queue
        readResults.stream()
                .forEach(x -> queueAddressManager.addToResolvedQueue(x.getGlobalAddress()));

        // Update the global pointer
        if (!readResults.isEmpty()) {
            queueAddressManager.setGlobalPointerCheckGCTrimMark(readResults.get(readResults.size() - 1)
                    .getGlobalAddress());
        }

        return readResults;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return getHasNext();
    }

    /**
     * {@inheritDoc}
     * */
    @Override
    public long getCurrentGlobalPosition() {
        return queueAddressManager.getGlobalPointer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void seek(long globalAddress) {
        queueAddressManager.seek(globalAddress);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long find(long globalAddress, SearchDirection direction) {
        // First, check if we have resolved up to the given address
        if (queueAddressManager.getMaxResolution() < globalAddress) {
            // If not we need to read to that position
            // to resolve all the addresses.
            remainingUpTo(globalAddress + 1);
        }

        // Now we can do the search.
        // First, check for inclusive searches.
        if (direction.isInclusive()
                && queueAddressManager.resolvedQueue.contains(globalAddress)) {
            return globalAddress;
        }
        // Next, check all elements excluding
        // in the correct direction.
        Long result;
        if (direction.isForward()) {
            result = queueAddressManager.resolvedQueue.higher(globalAddress);
        }  else {
            result = queueAddressManager.resolvedQueue.lower(globalAddress);
        }

        // Convert the address to never read if there was no result.
        return result == null ? Address.NOT_FOUND : result;
    }

    @Override
    public void reset() {
        queueAddressManager.reset();
    }

    @Override
    public void gc(long trimMark) {
        queueAddressManager.gc(trimMark);
    }

    @Override
    public long getTotalUpdates() {
        return updateCounter;
    }

    /**
     * Reads data from an address in the address space. It will give the writer a chance to complete based on the time
     * at which this individual read started. If the reads have been going on for longer than
     * the grace period given for a writer to complete a write, the subsequent individual read calls will immediately
     * fill the hole on absence of data at the given address.
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
     * @return log data at the address.
     */
    protected ILogData read(final long address, long readStartTime) {
        try {
            if (System.currentTimeMillis() - readStartTime <
                    runtime.getParameters().getHoleFillTimeout().toMillis()) {
                return runtime.getAddressSpaceView().read(address);
            }
            return runtime.getAddressSpaceView()
                    .read(Collections.singleton(address), false)
                    .get(address);
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }

    /**
     * Read a list of addresses.
     *
     * This will wait for writes to complete based on the grace period given for writers,
     * otherwise, hole filling will take place to unblock readers from slow writers.
     *
     * @param addresses list of addresses to read.
     * @return list of data for each of the requested addresses.
     */
    protected @Nonnull List<ILogData> readAll(@Nonnull List<Long> addresses) {
        try {
            Map<Long, ILogData> dataMap =
                    runtime.getAddressSpaceView().read(addresses);
            return addresses.stream().map(dataMap::get).collect(Collectors.toList());
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }

    /**
     * Determine if a trim exception should not be retriable.
     *
     * @param te Trim Exception
     */
    private void processTrimmedException(TrimmedException te) {
        if (TransactionalContext.getCurrentContext() != null
                && TransactionalContext.getCurrentContext().getSnapshotTimestamp().getSequence()
                < queueAddressManager.getCheckpointSuccessStartAddr()) {
            // Do not retry transaction. if the transaction's snapshot is lower than the checkpoint
            // snapshot start address, as there is absolute guarantee that there is no resolution
            // under this checkpoint.
            te.setRetriable(false);
        }
    }

    /**
     * Fill the read queue for the current stream. This method is called
     * whenever a client requests a read, but there are no addresses left in
     * the read queue.
     *
     * @param maxGlobal     The maximum global address to read to.
     * @return              True, if entries were added to the read queue,
     *                      False, otherwise.
     */
    public boolean fillReadQueue(final long maxGlobal) {
        log.trace("fillReadQueue[{}] Max: {}, Current: {}, Resolved: {} - {}", this,
                maxGlobal, queueAddressManager.getGlobalPointer(), queueAddressManager.getMaxResolution(),
                queueAddressManager.getMinResolution());
        log.trace("fillReadQueue[{}]: addresses in this stream Resolved queue {}" +
                        " - ReadQueue {} - CP Queue {}", this,
                queueAddressManager.resolvedQueue, queueAddressManager.readQueue, queueAddressManager.readCpQueue);

        // If the stream has just been reset and we don't have any checkpoint entries,
        // we should consult a checkpoint first.
        if (queueAddressManager.isAddressSpaceReset()) {
            boolean checkpointFound = discoverCheckpoint(maxGlobal);
            if (checkpointFound) {
                return true;
            }
        }

        // Discover regular stream
        // If we already reached maxAddress ,
        // we return since there is nothing left to do.
        if (queueAddressManager.getGlobalPointer() >= maxGlobal) {
            return false;
        }

        // If everything is available in the resolved queue, use it
        if(queueAddressManager.isAddressSpaceResolvedUpTo(maxGlobal)) {
            return fillFromResolved(maxGlobal);
        }

        Long latestTokenValue = null;

        // If the max has been resolved, use it.
        if (maxGlobal != Address.MAX) {
            latestTokenValue = queueAddressManager.resolvedQueue.ceiling(maxGlobal);
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
            latestTokenValue = runtime.getSequencerView().query(id)
                    .getToken().getSequence();
            log.trace("fillReadQueue[{}] Fetched tail {} from sequencer", this, latestTokenValue);
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
        if (queueAddressManager.getMaxResolution() >= latestTokenValue
                && queueAddressManager.getMinResolution() < queueAddressManager.getGlobalPointer()) {
            return fillFromResolved(latestTokenValue);
        }

        long stopAddress = queueAddressManager.getMaxResolvedAddress();

        // We check if we can fill partially from the resolved queue
        // This is a requirement for the getStreamAddressMaps as it considers the content of the resolved queue
        // to decide if needs to fetch the address maps or not.
        if (queueAddressManager.getGlobalPointer() < queueAddressManager.getMaxResolution()
                && fillFromResolved(queueAddressManager.getMaxResolution())) {
            stopAddress = queueAddressManager.getMaxResolution();
            log.trace("fillReadQueue[{}]: current pointer: {}, resolved up to: {}, readQueue: {}, " +
                            "new stop address: {}", this, queueAddressManager.getGlobalPointer(),
                    queueAddressManager.getMaxResolution(), queueAddressManager.readQueue, stopAddress);
        }

        // Now we fetch the address map for this stream from the sequencer in a single call, i.e.,
        // addresses of this stream in the range (stopAddress, startAddress==latestTokenValue]
        discoverAddressSpace(id, latestTokenValue, stopAddress,
                d -> AbstractQueuedStreamView.BackpointerOp.INCLUDE, false, maxGlobal);

        return queueAddressManager.isAnyQueueFilled();
    }

    /**
     * Discover the checkpoint for this stream, covering the space for maxGlobal.
     *
     * @param maxGlobal
     * @return
     */
    private boolean discoverCheckpoint(long maxGlobal) {
        // The checkpoint stream ID is the UUID appended with CP
        final UUID checkpointId = CorfuRuntime
                .getCheckpointStreamIdFromId(id);
        // Find the checkpoint, if present
        try {
            boolean addressesFound = discoverAddressSpace(checkpointId,
                    runtime.getSequencerView().query(checkpointId).getToken().getSequence(),
                    Address.NEVER_READ, d -> resolveCheckpoint(d, maxGlobal), true, maxGlobal);
            if (addressesFound) {
                log.trace("fillReadQueue[{}] Get Stream Address Map using checkpoint with {} entries",
                        this, queueAddressManager.readCpQueue.size());

                return true;
            }
        } catch (TrimmedException te) {
            // If we reached a trim and didn't hit a checkpoint, this might be okay,
            // if the stream was created recently and no checkpoint exists yet.
            log.warn("fillReadQueue[{}] Trim encountered and no checkpoint detected.", this);
        }

        return false;
    }

    /**
     * Fill read queue from addresses available in the resolved queue, up to maxGlobal.
     *
     * @param maxGlobal maximum address to fill up to.
     * @return True, if read queue was filled.
     *         False, otherwise.
     */
    protected boolean fillFromResolved(final long maxGlobal) {
        // There's nothing to read if we're already past maxGlobal.
        if (maxGlobal < queueAddressManager.getGlobalPointer()) {
            return false;
        }
        // Get the subset of the resolved queue, which starts at
        // globalPointer and ends at maxAddress inclusive.
        NavigableSet<Long> resolvedSet =
                queueAddressManager.resolvedQueue.subSet(queueAddressManager.getGlobalPointer(),
                        false, maxGlobal, true);

        // Put those elements in the read queue
        queueAddressManager.readQueue.addAll(resolvedSet);

        return !queueAddressManager.readQueue.isEmpty();
    }

    /**
     *  {@inheritDoc}
     *
     **/
    protected ILogData read(final long address) {
        try {
            return runtime.getAddressSpaceView().read(address);
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }

    protected @Nonnull ILogData readRange(long address, @Nonnull final List<Long> addresses) {
        try {
            return runtime.getAddressSpaceView().predictiveReadRange(address, addresses);
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p> We indicate we may have entries available
     * if the read queue contains entries to read -or-
     * if the next token is greater than our log pointer.
     */
    public boolean getHasNext() {
        return  !queueAddressManager.readQueue.isEmpty()
                || runtime.getSequencerView().query(id).getToken().getSequence()
                > queueAddressManager.getGlobalPointer();
    }

    public void close() {}

    protected BackpointerOp resolveCheckpoint(ILogData data, long maxGlobal) {
        if (data.hasCheckpointMetadata()) {
            CheckpointEntry cpEntry = (CheckpointEntry)
                    data.getPayload(runtime);

            // Select the latest cp that has a snapshot address
            // which is less than maxGlobal
            if (queueAddressManager.getCheckpointSuccessId() == null &&
                    cpEntry.getCpType() == CheckpointEntry.CheckpointEntryType.END
                    && Long.decode(cpEntry.getDict()
                    .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)) <= maxGlobal) {
                log.trace("Checkpoint[{}] END found at address {} type {} id {} author {}",
                        this, data.getGlobalAddress(), cpEntry.getCpType(),
                        Utils.toReadableId(cpEntry.getCheckpointId()),
                        cpEntry.getCheckpointAuthorId());
                queueAddressManager.setCheckpointSuccessId(cpEntry.getCheckpointId());
            }
            else if (data.getCheckpointId().equals(queueAddressManager.getCheckpointSuccessId())) {
                if (cpEntry.getCpType().equals(CheckpointEntry.CheckpointEntryType.START)) {
                    queueAddressManager.setCheckpointSuccessStartAddr(Long.decode(cpEntry.getDict()
                            .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)));
                    if (cpEntry.getDict().get(CheckpointEntry.CheckpointDictKey
                            .SNAPSHOT_ADDRESS) != null) {
                        queueAddressManager.setCheckpointSnapshotAddress(Long.decode(cpEntry.getDict()
                                .get(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS)));
                    }
                    log.trace("Checkpoint[{}] HALT due to START at address {} startAddr"
                                    + " {} type {} id {} author {}",
                            this, data.getGlobalAddress(), queueAddressManager.getCheckpointSuccessStartAddr(),
                            cpEntry.getCpType(),
                            Utils.toReadableId(cpEntry.getCheckpointId()),
                            cpEntry.getCheckpointAuthorId());
                    return BackpointerOp.INCLUDE_STOP;
                }
            } else {
                return BackpointerOp.EXCLUDE;
            }
        }
        return BackpointerOp.INCLUDE;
    }

    @VisibleForTesting
    QueueAddressManager getQueueAddressManager() {
        return this.queueAddressManager;
    }
}
