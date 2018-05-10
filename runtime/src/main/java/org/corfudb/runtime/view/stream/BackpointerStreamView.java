package org.corfudb.runtime.view.stream;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AppendException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.util.Utils;


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
    }

    public BackpointerStreamView(final CorfuRuntime runtime,
                                 final UUID streamId) {
        super(runtime, streamId);
        this.options = StreamOptions.DEFAULT;
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
        // First, we get a token from the sequencer.
        TokenResponse tokenResponse = runtime.getSequencerView()
                .nextToken(Collections.singleton(id), 1);

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
                runtime.getAddressSpaceView()
                        .write(tokenResponse, object);
                // The write completed successfully, so we return this
                // address to the client.
                return tokenResponse.getToken().getTokenValue();
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
                tokenResponse = runtime.getSequencerView()
                        .nextToken(Collections.singleton(id),
                             1);
            } catch (StaleTokenException te) {
                log.trace("Token grew stale occurred at {}", tokenResponse);
                if (deacquisitionCallback != null && !deacquisitionCallback.apply(tokenResponse)) {
                        log.debug("Deacquisition requested abort");
                        return -1L;
                }
                // Request a new token, informing the sequencer we were
                // overwritten.
                tokenResponse = runtime.getSequencerView()
                        .nextToken(Collections.singleton(id),
                                1);

            }
        }

        log.error("append[{}]: failed after {} retries, write size {} bytes",
                tokenResponse.getTokenValue(),
                runtime.getParameters().getWriteRetry(),
                ILogData.getSerializedSize(object));
        throw new AppendException();
    }

    void processTrimmedException(TrimmedException te) {
        if (TransactionalContext.getCurrentContext() != null
                && TransactionalContext.getCurrentContext().getSnapshotTimestamp()
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
                || runtime.getSequencerView()
                .nextToken(Collections.singleton(context.id), 0).getToken().getTokenValue()
                        > context.globalPointer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {}


    protected boolean fillFromResolved(final long maxGlobal,
                                       final QueuedStreamContext context) {
        // There's nothing to read if we're already past maxGlobal.
        if (maxGlobal < context.globalPointer) {
            return false;
        }
        // Get the subset of the resolved queue, which starts at
        // globalPointer and ends at maxAddress inclusive.
        NavigableSet<Long> resolvedSet =
                context.resolvedQueue.subSet(context.globalPointer,
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

    protected boolean followBackpointers(final UUID streamId,
                                      final NavigableSet<Long> queue,
                                      final long startAddress,
                                      final long stopAddress,
                                      final Function<ILogData, BackpointerOp> filter) {
        log.trace("followBackPointers: stmreadId[{}], queue[{}], startAddress[{}], stopAddress[{}]," +
                "filter[{}]", streamId, queue, startAddress, stopAddress, filter);
        // Whether or not we added entries to the queue.
        boolean entryAdded = false;
        // The current address which we are reading from.
        long currentAddress = startAddress;

        // Loop until we have reached the stop address.
        while (currentAddress > stopAddress  && Address.isAddress(currentAddress)) {
            backpointerCount++;

            // Read the current address
            ILogData d;
            try {
                log.trace("followBackPointers: readAddress[{}]", currentAddress);
                d = read(currentAddress);
            } catch (TrimmedException e) {
                if (options.ignoreTrimmed) {
                    log.warn("followBackpointers: Ignoring trimmed exception for address[{}]," +
                            " stream[{}]", currentAddress, id);
                    return entryAdded;
                } else {
                    throw e;
                }
            }

            // If it contains the stream we are interested in
            if (d.containsStream(streamId)) {
                log.trace("followBackPointers: address[{}] contains streamId[{}], apply filter", currentAddress,
                        streamId);
                // Check whether we should include the address
                BackpointerOp op = filter.apply(d);
                if (op == BackpointerOp.INCLUDE
                        || op == BackpointerOp.INCLUDE_STOP) {
                    log.trace("followBackPointers: Adding backpointer to address[{}] to queue", currentAddress);
                    queue.add(currentAddress);
                    entryAdded = true;
                    // Check if we need to stop
                    if (op == BackpointerOp.INCLUDE_STOP) {
                        return entryAdded;
                    }
                }
            }

            boolean singleStep = true;
            // Now calculate the next address
            // Try using backpointers first

            log.trace("followBackPointers: calculate the next address");

            if (!runtime.getParameters().isBackpointersDisabled() && d.hasBackpointer(streamId)) {
                long tmp = d.getBackpointer(streamId);
                log.trace("followBackPointers: backpointer points to {}", tmp);
                // if backpointer is a valid log address or Address.NON_EXIST
                // (beginning of the stream), do not single step back on the log
                if (Address.isAddress(tmp) || tmp == Address.NON_EXIST) {
                    currentAddress = tmp;
                    singleStep = false;
                }
            }

            if (singleStep) {
                // backpointers failed, so we're
                // downgrading to a linear scan
                log.trace("followBackPointers: downgrading to single step, backpointer failed");
                currentAddress = currentAddress - 1;
            }
        }

        return entryAdded;

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
        log.trace("Read_Fill_Queue[{}] Max: {}, Current: {}, Resolved: {} - {}", this,
                maxGlobal, context.globalPointer, context.maxResolution, context.minResolution);

        // If the stream has just been reset and we don't have
        // any checkpoint entries, we should consult
        // a checkpoint first.
        if (context.globalPointer == Address.NEVER_READ &&
                context.checkpointSuccessId == null) {
            // The checkpoint stream ID is the UUID appended with CP
            final UUID checkpointId = CorfuRuntime
                    .getCheckpointStreamIdFromId(context.id);
            // Find the checkpoint, if present
            try {
                if (followBackpointers(checkpointId, context.readCpQueue,
                        runtime.getSequencerView()
                                .nextToken(Collections.singleton(checkpointId), 0)
                                .getToken().getTokenValue(),
                        Address.NEVER_READ, d -> resolveCheckpoint(context, d, maxGlobal))) {
                    log.trace("Read_Fill_Queue[{}] Using checkpoint with {} entries",
                            this, context.readCpQueue.size());
                    return true;
                }
            } catch (TrimmedException te) {
                // If we reached a trim and didn't hit a checkpoint, this might be okay,
                // if the stream was created recently and no checkpoint exists yet.
                log.warn("Read_Fill_Queue[{}] Trim encountered and no checkpoint detected.", this);
            }
        }

        // The maximum address we will fill to.
        final long maxAddress =
                Long.min(maxGlobal, context.maxGlobalAddress);

        // If we already reached maxAddress ,
        // we return since there is nothing left to do.
        if (context.globalPointer >= maxAddress) {
            return false;
        }

        // If everything is available in the resolved
        // queue, use it
        if (context.maxResolution >= maxAddress
                && context.minResolution < context.globalPointer) {
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
            latestTokenValue = runtime.getSequencerView()
                    .nextToken(Collections.singleton(context.id), 0)
                    .getToken().getTokenValue();
            log.trace("Read_Fill_Queue[{}] Fetched tail {} from sequencer", this, latestTokenValue);
        }
        // If there is no information on the tail of the stream, return,
        // there is nothing to do
        if (Address.nonAddress(latestTokenValue)) {

            // sanity check:
            // curretly, the only possible non-address return value for a token-query
            // is Address.NON_EXIST
            if (latestTokenValue != Address.NON_EXIST) {
                log.warn("TOKEN[{}] unexpected return value", latestTokenValue);
            }

            return false;
        }

        // If everything is available in the resolved
        // queue, use it
        if (context.maxResolution >= latestTokenValue
                && context.minResolution < context.globalPointer) {
            return fillFromResolved(latestTokenValue, context);
        }

        // Now we start traversing backpointers, if they are available. We
        // start at the latest token and go backward, until we reach the
        // log pointer -or- the checkpoint snapshot address, because all
        // values from the beginning of the stream up to the snapshot address
        // should be reflected. For each address which is less than
        // maxGlobalAddress, we insert it into the read queue.

        followBackpointers(context.id, context.readQueue,
                latestTokenValue,
                Long.max(context.globalPointer, context.checkpointSnapshotAddress),
                d -> BackpointerOp.INCLUDE);

        return ! context.readCpQueue.isEmpty() || !context.readQueue.isEmpty();
    }
}

