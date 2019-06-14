package org.corfudb.runtime.view.stream;

import com.google.common.annotations.VisibleForTesting;
import lombok.ToString;
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
import org.corfudb.util.Utils;

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

    /** Create a new queued stream view.
     *
     * @param streamId  The ID of the stream
     * @param runtime   The runtime used to create this view.
     */
    public AbstractQueuedStreamView(final CorfuRuntime runtime,
                                    final UUID streamId) {
        super(runtime, streamId, QueuedStreamContext::new);
    }

    /** Add the given address to the resolved queue of the
     * given context.
     * @param context           The context to add the address to
     * @param globalAddress     The resolved global address.
     */
    protected void addToResolvedQueue(QueuedStreamContext context,
                                      long globalAddress,
                                      ILogData ld) {
        context.resolvedQueue.add(globalAddress);
        context.resolvedEstBytes += ld.getSizeEstimate();

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
        if (context.readQueue.isEmpty() && context.readCpQueue.isEmpty()
                && !fillReadQueue(maxGlobal, context)) {
            return null;
        }

        // If maxGlobal is before the checkpoint position, throw a
        // trimmed exception
        if (maxGlobal < context.checkpointSuccessStartAddr) {
            throw new TrimmedException();
        }

        // If checkpoint data is available, get from readCpQueue first
        NavigableSet<Long> getFrom;
        if (context.readCpQueue.size() > 0) {
            getFrom = context.readCpQueue;
            // Note: this is a checkpoint, we do not need to verify it is before the trim mark, it actually should be
            // cause this is the last address of the trimmed range.
            context.setGlobalPointer(context.checkpointSuccessStartAddr);
        } else {
            getFrom = context.readQueue;
        }

        // If the lowest DATA element is greater than maxGlobal, there's nothing
        // to return.
        if (context.readCpQueue.isEmpty() && context.readQueue.first() > maxGlobal) {
            return null;
        }

        return removeFromQueue(getFrom);
    }

    /**
     * Remove next entry from the queue.
     *
     * @param queue queue of entries.
     * @return next available entry. or null if there are no more entries
     *         or remaining entries are not part of this stream.
     */
    protected abstract ILogData removeFromQueue(NavigableSet<Long> queue);

    @Override
    public void gc(long trimMark) {
        // GC stream only if the pointer is ahead from the trim mark (last untrimmed address),
        // this guarantees that the data to be discarded is already applied to the stream and data will not be lost.
        // Note: if the pointer is behind, discarding the data immediately will incur in data
        // loss as checkpoints are only loaded on resets. We don't want to trigger resets as this slows
        // the runtime.
        if (getCurrentContext().getGlobalPointer() >= getCurrentContext().getGcTrimMark()) {
            log.debug("gc[{}]: start GC on stream {} for trim mark {}", this, this.getId(),
                    getCurrentContext().getGcTrimMark());
            // Remove all the entries that are strictly less than
            // the trim mark
            getCurrentContext().readCpQueue.headSet(getCurrentContext().getGcTrimMark()).clear();
            getCurrentContext().readQueue.headSet(getCurrentContext().getGcTrimMark()).clear();
            getCurrentContext().resolvedQueue.headSet(getCurrentContext().getGcTrimMark()).clear();

            if (!getCurrentContext().resolvedQueue.isEmpty()) {
                getCurrentContext().minResolution = getCurrentContext()
                        .resolvedQueue.first();
            }
        } else {
            log.debug("gc[{}]: GC not performed on stream {} for this cycle. Global pointer {} is below trim mark {}",
                    this, this.getId(), getCurrentContext().getGlobalPointer(), getCurrentContext().getGcTrimMark());
        }

        // Set the trim mark for next GC Cycle
        getCurrentContext().setGcTrimMark(trimMark);
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
        final LogData ld = new LogData(DataType.DATA, object);
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
                ILogData.getSerializedSize(object));
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

    @Nonnull
    protected List<ILogData> readAll(@Nonnull List<Long> addresses) {
        try {
            Map<Long, ILogData> dataMap =
                    runtime.getAddressSpaceView().read(addresses);
            return addresses.stream().map(dataMap::get).collect(Collectors.toList());
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }

    private void processTrimmedException(TrimmedException te) {
        if (TransactionalContext.getCurrentContext() != null
                && TransactionalContext.getCurrentContext().getSnapshotTimestamp().getSequence()
                < getCurrentContext().checkpointSnapshotAddress) {
            te.setRetriable(false);
        }
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

        // If maxGlobal is before the checkpoint position, throw a
        // trimmed exception
        if (maxGlobal < context.checkpointSuccessStartAddr) {
            throw new TrimmedException();
        }

        // We always have to fill to the read queue to ensure we read up to
        // max global.
        if (readQueueIsEmpty) {
            return Collections.emptyList();
        }

        // If we witnessed a checkpoint during our scan that
        // we should pay attention to, then start with them.
        readSet.addAll(context.readCpQueue);

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
        List<ILogData> readFrom = readAll(toRead).stream()
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
                .forEach(x -> addToResolvedQueue(context, x.getGlobalAddress(), x));

        // Update the global pointer
        if (readFrom.size() > 0) {
            context.setGlobalPointerCheckGCTrimMark(readFrom.get(readFrom.size() - 1)
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
                        " - ReadQueue {} - CP Queue {}", this,
                context.resolvedQueue, context.readQueue, context.readCpQueue);

        // If the stream has just been reset and we don't have
        // any checkpoint entries, we should consult
        // a checkpoint first.
        if (context.getGlobalPointer() == Address.NEVER_READ &&
                context.checkpointSuccessId == null) {
            // The checkpoint stream ID is the UUID appended with CP
            final UUID checkpointId = CorfuRuntime
                    .getCheckpointStreamIdFromId(context.id);
            // Find the checkpoint, if present
            try {
                if (discoverAddressSpace(checkpointId, context.readCpQueue,
                        runtime.getSequencerView()
                                .query(checkpointId).getToken().getSequence(),
                        Address.NEVER_READ, d -> resolveCheckpoint(context, d, maxGlobal),
                        true, maxGlobal)) {
                    log.trace("Fill_Read_Queue[{}] Get Stream Address Map using checkpoint with {} entries",
                            this, context.readCpQueue.size());

                    return true;
                }
            } catch (TrimmedException te) {
                log.warn("Fill_Read_Queue[{}] Trim encountered.", this);
                throw te;
            }
        }

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
            latestTokenValue = runtime.getSequencerView().query(context.id)
                    .getToken().getSequence();
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

        long stopAddress = Long.max(context.globalPointer, context.checkpointSnapshotAddress);

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
                d -> BackpointerOp.INCLUDE, false, maxGlobal);

        return !context.readCpQueue.isEmpty() || !context.readQueue.isEmpty();
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
     * @param checkpoint true if checkpoint discovery, false otherwise.
     * @param maxGlobal max address to resolve discovery.
     *
     * @return true if addresses were discovered, false, otherwise.
     */
    protected abstract boolean discoverAddressSpace(final UUID streamId,
                                                    final NavigableSet<Long> queue,
                                                    final long startAddress,
                                                    final long stopAddress,
                                                    final Function<ILogData, BackpointerOp> filter,
                                                    final boolean checkpoint,
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
    @Override
    public boolean getHasNext(QueuedStreamContext context) {
        return  !context.readQueue.isEmpty()
                || runtime.getSequencerView().query(context.id).getToken().getSequence()
                > context.getGlobalPointer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {}

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long find(long globalAddress, SearchDirection direction) {
        final QueuedStreamContext context = getCurrentContext();
        // First, check if we have resolved up to the given address
        if (context.maxResolution < globalAddress) {
            // If not we need to read to that position
            // to resolve all the addresses.
            remainingUpTo(globalAddress + 1);
        }

        // Now we can do the search.
        // First, check for inclusive searches.
        if (direction.isInclusive()
                && context.resolvedQueue.contains(globalAddress)) {
            return globalAddress;
        }
        // Next, check all elements excluding
        // in the correct direction.
        Long result;
        if (direction.isForward()) {
            result = context.resolvedQueue.higher(globalAddress);
        }  else {
            result = context.resolvedQueue.lower(globalAddress);
        }

        // Convert the address to never read if there was no result.
        return result == null ? Address.NOT_FOUND : result;
    }

    protected BackpointerOp resolveCheckpoint(final QueuedStreamContext context, ILogData data,
                                              long maxGlobal) {
        if (data.hasCheckpointMetadata()) {
            CheckpointEntry cpEntry = (CheckpointEntry)
                    data.getPayload(runtime);

            // Select the latest cp that has a snapshot address
            // which is less than maxGlobal
            if (context.checkpointSuccessId == null &&
                    cpEntry.getCpType() == CheckpointEntry.CheckpointEntryType.END
                    && Long.decode(cpEntry.getDict()
                    .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)) <= maxGlobal) {
                log.trace("Checkpoint[{}] END found at address {} type {} id {} author {}",
                        this, data.getGlobalAddress(), cpEntry.getCpType(),
                        Utils.toReadableId(cpEntry.getCheckpointId()),
                        cpEntry.getCheckpointAuthorId());
                context.checkpointSuccessId = cpEntry.getCheckpointId();

                context.checkpointSuccessNumEntries = 1L;
                context.checkpointSuccessBytes = (long) data.getSizeEstimate();
                context.checkpointSuccessEndAddr = data.getGlobalAddress();
            }
            else if (data.getCheckpointId().equals(context.checkpointSuccessId)) {
                context.checkpointSuccessNumEntries++;
                context.checkpointSuccessBytes += cpEntry.getSmrEntriesBytes();
                if (cpEntry.getCpType().equals(CheckpointEntry.CheckpointEntryType.START)) {
                    context.checkpointSuccessStartAddr = Long.decode(cpEntry.getDict()
                            .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS));
                    if (cpEntry.getDict().get(CheckpointEntry.CheckpointDictKey
                            .SNAPSHOT_ADDRESS) != null) {
                        context.checkpointSnapshotAddress = Long.decode(cpEntry.getDict()
                                .get(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS));
                    }
                    log.trace("Checkpoint[{}] HALT due to START at address {} startAddr"
                                    + " {} type {} id {} author {}",
                            this, data.getGlobalAddress(), context.checkpointSuccessStartAddr,
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

        // If we're attempt to go prior to most recent checkpoint, we
        // throw a TrimmedException.
        if (context.getGlobalPointer() - 1 < context.checkpointSuccessStartAddr) {
            throw new TrimmedException();
        }

        // Otherwise, the previous entry should be resolved, so get
        // one less than the current.
        Long prevAddress = context
                .resolvedQueue.lower(context.getGlobalPointer());
        // If the pointer is before our min resolution, we need to resolve
        // to get the correct previous entry.
        if (prevAddress == null && Address.isAddress(context.minResolution)
                || prevAddress != null && prevAddress <= context.minResolution) {
            context.setGlobalPointerCheckGCTrimMark(prevAddress == null ? Address.NEVER_READ :
                    prevAddress - 1L);

            remainingUpTo(context.minResolution);
            context.minResolution = Address.NON_ADDRESS;
            context.setGlobalPointerCheckGCTrimMark(oldPointer);
            prevAddress = context
                    .resolvedQueue.lower(context.getGlobalPointer());
            log.trace("previous[{}]: updated resolved queue {}", this, context.resolvedQueue);
        }

        // Clear the read queue, it may no longer be valid
        context.readQueue.clear();

        if (prevAddress != null) {
            log.trace("previous[{}]: updated read queue {}", this, context.readQueue);
            context.setGlobalPointerCheckGCTrimMark(prevAddress);
            return read(prevAddress);
        }

        if (context.checkpointSuccessId == null) {
            // The stream hasn't been checkpointed and we need to
            // move the stream pointer to an address before the first
            // entry
            log.trace("previous[{}]: reached the beginning of the stream resetting" +
                    " the stream pointer to {}", this, Address.NON_ADDRESS);
            context.setGlobalPointerCheckGCTrimMark(Address.NON_ADDRESS);
            return null;
        }

        if (context.resolvedQueue.first() == context.getGlobalPointer()) {
            log.trace("previous[{}]: reached the beginning of the stream resetting" +
                    " the stream pointer to checkpoint version {}", this, context.checkpointSuccessStartAddr);
            // Note: this is a checkpoint, we do not need to verify it is before the trim mark, it actually should be
            // cause this is the last address of the trimmed range.
            context.setGlobalPointer(context.checkpointSuccessStartAddr);
            return null;
        }

        throw new IllegalStateException("The stream pointer seems to be corrupted!");
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
        return read(context.getGlobalPointer());
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

    protected enum BackpointerOp {
        INCLUDE,    /** Include this address. */
        EXCLUDE,    /** Exclude this address. */
        INCLUDE_STOP    /** Stop, but also include this address. */
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

        /** List of checkpoint records, if a successful checkpoint has been observed.
         */
        final NavigableSet<Long> readCpQueue = new TreeSet<>();

        /** Info on checkpoint we used for initial stream replay,
         *  other checkpoint-related info & stats.  Hodgepodge, clarify.
         */
        UUID checkpointSuccessId = null;
        long checkpointSuccessStartAddr = Address.NEVER_READ;
        long checkpointSuccessEndAddr = Address.NEVER_READ;
        long checkpointSuccessNumEntries = 0L;
        long checkpointSuccessBytes = 0L;
        // No need to keep track of # of DATA entries, use context.resolvedQueue.size()?
        long resolvedEstBytes = 0L;
        /** The address the current checkpoint snapshot was taken at.
         *  The checkpoint guarantees for this stream there are no entries
         *  between checkpointSuccessStartAddr and checkpointSnapshotAddress.
         */
        long checkpointSnapshotAddress = Address.NEVER_READ;

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
            readCpQueue.clear();
            readQueue.clear();
            resolvedQueue.clear();
            minResolution = Address.NON_ADDRESS;
            maxResolution = Address.NON_ADDRESS;

            checkpointSuccessId = null;
            checkpointSuccessStartAddr = Address.NEVER_READ;
            checkpointSuccessEndAddr = Address.NEVER_READ;
            checkpointSnapshotAddress = Address.NEVER_READ;
            checkpointSuccessNumEntries = 0;
            checkpointSuccessBytes = 0;
            resolvedEstBytes = 0;
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
