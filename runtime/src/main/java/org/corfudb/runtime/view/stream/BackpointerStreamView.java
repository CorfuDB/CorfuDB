package org.corfudb.runtime.view.stream;

import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Range;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamAddressSpace;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AppendException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.replication.ChainReplicationProtocol;
import org.corfudb.util.Utils;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;


/** A view of a stream implemented with backpointers.
 *
 * <p>In this implementation, all addresses are global (log) addresses.
 *
 * <p>All method calls of this class are thread-safe.
 *
 * <p>Created by mwei on 12/11/15.
 */
@Slf4j
public class BackpointerStreamView extends AbstractQueuedStreamView {

    final StreamOptions options;

    /**
     * Max write limit.
     */
    final int maxWrite;

    /** Create a new backpointer stream view.
     *
     * @param runtime   The runtime to use for accessing the log.
     * @param streamId  The ID of the stream to view.
     */
    public BackpointerStreamView(final CorfuRuntime runtime,
                                 final UUID streamId,
                                 @Nonnull final StreamOptions options) {
        super(runtime, streamId);
        this.options = options;
        maxWrite = runtime.getParameters().getMaxWriteSize();
    }

    public BackpointerStreamView(final CorfuRuntime runtime,
                                 final UUID streamId) {
        this(runtime, streamId, StreamOptions.DEFAULT);
    }

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
     * <p>In the backpointer-based implementation, we loop forever trying to
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

    void processTrimmedException(TrimmedException te) {
        if (TransactionalContext.getCurrentContext() != null
                && TransactionalContext.getCurrentContext().getSnapshotTimestamp().getSequence()
                < getCurrentContext().checkpointSnapshotAddress) {
            te.setRetriable(false);
        }
    }

    /** {@inheritDoc}
     *
     * <p>The backpointer version of remaining() calls nextUpTo() multiple times,
     * as it uses the default implementation in IStreamView. While this may
     * appear to be non-optimized, these reads will most likely hit in the
     * address space cache since the entries were read in order to resolve the
     * backpointers.
     *
     * */
    @Override
    protected ILogData read(final long address) {
        try {
            return runtime.getAddressSpaceView().read(address);
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }

    @Override
    protected @Nonnull ILogData readRange(final long address, @Nonnull final List<Long> addresses) {
        try {
            return runtime.getAddressSpaceView().predictiveReadRange(address, addresses);
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }


    /**
     * Reads data from an address in the address space. It will give the writer a chance to complete based on the time
     * when the reads of which this individual read is a step started. If the reads have been going on for longer than
     * the grace period given for a writer to complete a write, the subsequent individual read calls will immediately
     * fill the hole on absence of data at the given address.
     *
     * @param address       Address to read.
     * @param readStartTime Start time of the range of reads.
     * @return ILogData at the address.
     */
    private ILogData read(final long address, long readStartTime) {
        if (System.currentTimeMillis() - readStartTime < runtime.getParameters().getHoleFillTimeout().toMillis()) {
            try {
                return runtime.getAddressSpaceView().read(address);
            } catch (TrimmedException te) {
                processTrimmedException(te);
                throw te;
            }
        } else {
            RuntimeLayout runtimeLayout = runtime.getLayoutView().getRuntimeLayout();
            ChainReplicationProtocol replicationProtocol = (ChainReplicationProtocol) runtime
                    .getLayoutView()
                    .getLayout()
                    .getReplicationMode(address)
                    .getReplicationProtocol(runtime);
            return replicationProtocol
                    .readRange(runtimeLayout, Range.encloseAll(Arrays.asList(address)), false)
                    .get(address);

        }
    }

    @Nonnull
    @Override
    protected List<ILogData> readAll(@Nonnull List<Long> addresses) {
        try {
            Map<Long, ILogData> dataMap =
                    runtime.getAddressSpaceView().read(addresses);
            return addresses.stream()
                    .map(x -> dataMap.get(x))
                    .collect(Collectors.toList());
        } catch (TrimmedException te) {
            processTrimmedException(te);
            throw te;
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>In the backpointer based implementation, we indicate we may have
     * entries available if the read queue contains entries to read -or-
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

    protected enum BackpointerOp {
        INCLUDE,    /** Include this address. */
        EXCLUDE,    /** Exclude this address. */
        INCLUDE_STOP    /** Stop, but also include this address. */
    }

    private long backpointerCount = 0L;

    public long getBackpointerCount() {
        return backpointerCount;
    }

    /**
     * Retrieve this stream's backpointer map, i.e., a map of all addresses corresponding to this stream between
     * (stop address, start address] and return a boolean indicating if addresses were found in this range.
     *
     * @param streamId stream's ID, it is required because this same methof can be used for
     *                 discovering addresses from the checkpoint stream.
     * @param queue    queue to insert discovered addresses in the given range
     * @param startAddress range start address (inclusive)
     * @param stopAddress  range end address (exclusive and lower than start address)
     * @param filter       function to filter entries
     * @param checkpoint   boolean indicating if it is a checkpoint stream
     * @param maxGlobal    maximum Address until which we want to sync (is not necessarily equal to start address)
     * @return
     */
    protected boolean getStreamAddressMap(final UUID streamId,
                                          final NavigableSet<Long> queue,
                                          final long startAddress,
                                          final long stopAddress,
                                          final Function<ILogData, BackpointerOp> filter,
                                          final boolean checkpoint,
                                          final long maxGlobal) {
        // Sanity check: startAddress must be a valid address and greater than stopAddress.
        // The startAddress refers to the max resolution we want to resolve this stream to,
        // while stopAddress refers to the lower boundary locally resolved.
        // If startAddress is equal to stopAddress there is nothing to resolve.
        if (Address.isAddress(startAddress) && (startAddress > stopAddress)) {

            StreamAddressSpace streamAddressSpace = null;

            // If start and stop addresses are consecutive (e.g. 2 and 1) there is no need to search for this stream's
            // address map, as there are no other addresses within this range.
            if (startAddress - stopAddress > 1) {
                log.trace("getStreamAddressMap[{}] from {} to {}", this, startAddress, stopAddress);
                // This step is an optimization.
                // Attempt to read the last entry and verify its backpointer,
                // if this address is already resolved locally do not request the stream map to the sequencer as new
                // updates are not required to be synced.
                ILogData d;
                try {
                    log.trace("getStreamAddressMap: readAddress[{}]", startAddress);
                    d = read(startAddress);
                } catch (TrimmedException e) {
                    if (options.ignoreTrimmed) {
                        log.warn("getStreamAddressMap: Ignoring trimmed exception for address[{}]," +
                                " stream[{}]", startAddress, streamId);
                        return false;
                    } else {
                        throw e;
                    }
                }

                boolean rangeAddressToResolve = true;

                // Verify if backpointer is an address locally resolved
                if (!runtime.getParameters().isBackpointersDisabled() && d.hasBackpointer(streamId)) {
                    long previousAddress = d.getBackpointer(streamId);
                    log.trace("getStreamAddressMap[{}]: backpointer for {} points to {}", streamId, startAddress, previousAddress);
                    // if backpointer is a valid log address or Address.NON_EXIST (beginning of the stream)
                    if ((Address.isAddress(previousAddress) && getCurrentContext().resolvedQueue.contains(previousAddress)) || previousAddress == Address.NON_EXIST) {
                        log.trace("getStreamAddressMap[{}]: backpointer {} is locally present, do not request stream address map.", streamId, previousAddress);
                        streamAddressSpace = new StreamAddressSpace(Address.NON_ADDRESS, Roaring64NavigableMap.bitmapOf(startAddress));
                        rangeAddressToResolve = false;
                    }
                }

                if (rangeAddressToResolve) {
                    log.trace("getStreamAddressMap[{}]: request stream address space between {} and {}.", streamId, startAddress, stopAddress);
                    streamAddressSpace = runtime.getSequencerView().getStreamAddressSpace(new StreamAddressRange(streamId, startAddress, stopAddress));
                }
            } else {
                // Start and stop address are consecutive addresses, no need to request the address map for this stream,
                // only address to include is startAddress (stopAddress is already resolved - not included in the lookup).
                log.trace("getStreamAddressMap[{}]: request stream address space between {} and {}.", streamId, startAddress, stopAddress);
                streamAddressSpace = new StreamAddressSpace(Address.NON_ADDRESS, Roaring64NavigableMap.bitmapOf(startAddress));
            }

            if (checkpoint) {
                LongIterator revIterator = streamAddressSpace.getAddressMap().getReverseLongIterator();
                while (revIterator.hasNext()) {
                    Long cpAddress = revIterator.next();
                    ILogData d = read(cpAddress);
                    BackpointerOp op = filter.apply(d);
                    if (op == BackpointerOp.INCLUDE
                            || op == BackpointerOp.INCLUDE_STOP)
                    {
                        log.trace("getStreamAddressMap: Adding backpointer to address[{}] to queue", cpAddress);
                        queue.add(cpAddress);
                        // Check if we need to stop
                        if (op == BackpointerOp.INCLUDE_STOP) {
                            break;
                        }
                    }
                }
            } else {
                // Transfer discovered address space to queue, these are the addresses that upper layers will
                // resolve (apply) for the object.
                // We have to limit to maxGlobal, as startAddress could be ahead of maxGlobal if it reflects the tail
                // of the stream.
                log.trace("getStreamAddressMap[{}]: limit addresses to {}", this, maxGlobal);
                streamAddressSpace.copyAddressesToSet(queue, maxGlobal);
                log.trace("getStreamAddressMap[{}]: addresses in queue are {}", this, queue);
            }

            // Check if trimmed
            // TODO: I believe this optimization is not needed as the Address Space View handles Trimmed Exceptions
            // However, it allows to capture cases where a trimmed section of the log is not covered by a checkpoint
            // do we want to protect from these scenarios?
            if (Address.isAddress(streamAddressSpace.getTrimMark()) &&
                    ((getCurrentContext().checkpointSuccessId == null
                            && getCurrentContext().globalPointer < streamAddressSpace.getTrimMark()) ||
                            (getCurrentContext().checkpointSuccessStartAddr < streamAddressSpace.getTrimMark()
                                    && getCurrentContext().globalPointer < streamAddressSpace.getTrimMark() &&
                                    getCurrentContext().checkpointSuccessId != null))
                    ) {
                if (options.ignoreTrimmed) {
                    log.warn("getStreamAddressMap: Ignoring trimmed exception for address[{}]," +
                            " stream[{}]", streamAddressSpace.getTrimMark(), id);
                } else {
                    log.warn(String.format("Stream %s has been trimmed at address %s and this space is not covered by checkpoint with start address %s.",
                            streamId, streamAddressSpace.getTrimMark(),  getCurrentContext().checkpointSuccessStartAddr));
                    throw new TrimmedException(String.format("Stream %s has been trimmed at address %s and this space is not covered by checkpoint with start address %s.",
                            streamId, streamAddressSpace.getTrimMark(),  getCurrentContext().checkpointSuccessStartAddr));
                }
            }
        }

        // Count of backpointers only kept for unit test purpose, not actually relevant.
        if (!queue.isEmpty()) {
            backpointerCount += queue.size();
        }

        return !queue.isEmpty();
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
             && Long.decode(cpEntry.getDict().get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)) <= maxGlobal) {
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
     */
    @Override
    protected boolean fillReadQueue(final long maxGlobal,
                                 final QueuedStreamContext context) {
        log.trace("Fill_Read_Queue[{}] Max: {}, Current: {}, Resolved: {} - {}", this,
                maxGlobal, context.getGlobalPointer(), context.maxResolution, context.minResolution);
        log.trace("Fill_Read_Queue[{}]: addresses in this stream Resolved queue {} - ReadQueue {} - CP Queue {}", this,
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
                if (getStreamAddressMap(checkpointId, context.readCpQueue,
                        runtime.getSequencerView()
                                .query(checkpointId).getToken().getSequence(),
                        Address.NEVER_READ, d -> resolveCheckpoint(context, d, maxGlobal), true, maxGlobal)) {
                    log.trace("Fill_Read_Queue[{}] Using checkpoint with {} entries",
                            this, context.readCpQueue.size());

                    return true;
                }
            } catch (TrimmedException te) {
                // If we reached a trim and didn't hit a checkpoint, this might be okay,
                // if the stream was created recently and no checkpoint exists yet.
                log.warn("Fill_Read_Queue[{}] Trim encountered and no checkpoint detected.", this);
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

        // Now we start traversing backpointers, if they are available. We
        // start at the latest token and go backward, until we reach the
        // log pointer -or- the checkpoint snapshot address, because all
        // values from the beginning of the stream up to the snapshot address
        // should be reflected. For each address which is less than
        // maxGlobalAddress, we insert it into the read queue.
        long stopAddress = Long.max(context.globalPointer, context.checkpointSnapshotAddress);

        // Optimization: move from resolved queue to read queue available addresses.
        if(context.globalPointer < context.maxResolution) {
            if (fillFromResolved(context.maxResolution, context)) {
                Long highestAddress = context.readQueue.floor(context.maxResolution);
                if(highestAddress != null) {
                    // Set stop address to the maximum locally resolved address (change boundaries)
                    stopAddress = highestAddress;
                    log.trace("fillReadQueue[{}]: current pointer: {}, resolved up to: {}, readQueue: {}, " +
                                    "new stop address: {}", this, context.globalPointer,
                            context.maxResolution, context.readQueue, stopAddress);
                }
            }
        }

        getStreamAddressMap(context.id, context.readQueue,
                latestTokenValue,
                stopAddress,
                d -> BackpointerOp.INCLUDE, false, maxGlobal);

        return !context.readCpQueue.isEmpty() || !context.readQueue.isEmpty();
    }

    @VisibleForTesting
    AbstractQueuedStreamView.QueuedStreamContext getContext() {
        return this.baseContext;
    }
}

