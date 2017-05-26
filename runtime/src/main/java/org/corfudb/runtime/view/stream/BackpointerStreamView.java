package org.corfudb.runtime.view.stream;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Address;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** A view of a stream implemented with backpointers.
 *
 * In this implementation, all addresses are global (log) addresses.
 *
 * All method calls of this class are thread-safe.
 *
 * Created by mwei on 12/11/15.
 */
@Slf4j
public class BackpointerStreamView extends AbstractQueuedStreamView {

    /** Create a new backpointer stream view.
     *
     * @param runtime   The runtime to use for accessing the log.
     * @param streamID  The ID of the stream to view.
     */
    public BackpointerStreamView(final CorfuRuntime runtime,
                                 final UUID streamID) {
        super(runtime, streamID);
    }

    /**
     * {@inheritDoc}
     *
     * In the backpointer-based implementation, we loop forever trying to
     * write, and automatically retrying if we get overwritten (hole filled).
     */
    @Override
    public long append(Object object,
                       Function<TokenResponse, Boolean> acquisitionCallback,
                       Function<TokenResponse, Boolean> deacquisitionCallback) {
        // First, we get a token from the sequencer.
        TokenResponse tokenResponse = runtime.getSequencerView()
                .nextToken(Collections.singleton(ID), 1);

        // We loop forever until we are interrupted, since we may have to
        // acquire an address several times until we are successful.
        while (true) {
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
                        .nextToken(Collections.singleton(ID),
                             1);
            }
        }
    }

    /** {@inheritDoc}
     *
     * The backpointer version of remaining() calls nextUpTo() multiple times,
     * as it uses the default implementation in IStreamView. While this may
     * appear to be non-optimized, these reads will most likely hit in the
     * address space cache since the entries were read in order to resolve the
     * backpointers.
     *
     * */
    @Override
    protected ILogData read(final long address) {
            return runtime.getAddressSpaceView().read(address);
    }

    @Nonnull
    @Override
    protected List<ILogData> readAll(@Nonnull List<Long> addresses) {
        Map<Long, ILogData> dataMap =
            runtime.getAddressSpaceView().read(addresses);
        return addresses.stream()
                .map(x -> dataMap.get(x))
                .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     *
     * In the backpointer based implementation, we indicate we may have
     * entries available if the read queue contains entries to read -or-
     * if the next token is greater than our log pointer.
     */
    @Override
    public boolean getHasNext(QueuedStreamContext context) {
        return  !context.readQueue.isEmpty() ||
                runtime.getSequencerView()
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

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean fillReadQueue(final long maxGlobal,
                                 final QueuedStreamContext context) {
        log.trace("Read_Fill_Queue[{}] Max: {}, Current: {}, Resolved: {} - {}", this,
                maxGlobal, context.globalPointer, context.maxResolution, context.minResolution);

        // considerCheckpoint: Use context.globalPointer as a signal of caller's intent:
        // if globalPointer == -1, then the caller needs to replay the stream from the
        // beginning because the caller has never read anything from the stream before.
        // Thus, it could be a significant time & I/O saving for the client to playback
        // from the latest checkpoint.
        // On the other hand, if not -1, then we assume that the caller has already
        // found the stream head and replayed some/all of it.
        //
        // We assume a "typical" case where the client probably needs to discover & replay
        // just a few log entries, whereas the checkpoint data that this method may discover
        // may be "huge" ... thus we favor ignoring the checkpoint data.  Such an assumption
        // may be invalid for very small SMR objects, such as a simple counter, where
        // checkpoint size would always be small enough to use checkpoint data instead of
        // continuing backward to find individual updates.
        boolean considerCheckpoint = context.globalPointer == -1;

        // The maximum address we will fill to.
        final long maxAddress =
                Long.min(maxGlobal, context.maxGlobalAddress);

        // If the maximum address is less than the current pointer,
        // we return since there is nothing left to do.
        if (context.globalPointer > maxAddress) {
            return false;
        }

        // If everything is available in the resolved
        // queue, use it
        if (context.maxResolution > maxAddress &&
                context.minResolution < context.globalPointer) {
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
        }

        // If there is no infomation on the tail of the stream, return, there is nothing to do
        if (Address.nonAddress(latestTokenValue)) {

            // sanity check:
            // curretly, the only possible non-address return value for a token-query is Address.NON_EXIST
            if (latestTokenValue != Address.NON_EXIST)
                log.warn("TOKEN[{}] unexpected return value", latestTokenValue);

            return false;
        }

        // If everything is available in the resolved
        // queue, use it
        if (context.maxResolution > latestTokenValue &&
                context.minResolution < context.globalPointer) {
            return fillFromResolved(latestTokenValue, context);
        }

        // Now we start traversing backpointers, if they are available. We
        // start at the latest token and go backward, until we reach the
        // log pointer. For each address which is less than
        // maxGlobalAddress, we insert it into the read queue.
        long currentRead = latestTokenValue;

        while (currentRead > context.globalPointer &&
                Address.isAddress(currentRead)) {
            // We've somehow reached a read we already know about.
            if (context.readQueue.contains(currentRead)) {
                break;
            }

            log.trace("Read_Fill_Queue[{}] Read {}", this, currentRead);
            // Read the entry in question.
            ILogData currentEntry =
                    runtime.getAddressSpaceView().read(currentRead);

            // If the entry contains this context's stream,
            // we add it to the read queue.
            if (currentEntry.containsStream(context.id)) {
                if (currentEntry.hasCheckpointMetadata()) {
                    examineCheckpointRecord(context, currentEntry,
                            considerCheckpoint, currentRead);
                } else {
                    context.readQueue.add(currentRead);
                }
            }

            // If we've reached the beginning of a successful checkpoint
            // that the caller wants us to us, then we're done here.
            if (considerCheckpoint && currentRead <= context.checkpointSuccessStartAddr) {
                log.trace("Read_Fill_Queue[{}]: currentRead {} checkpointSuccessStartAddr {}",
                        this, currentRead, context.checkpointSuccessStartAddr);
                break;
            }

            // If everything left is available in the resolved
            // queue, use it
            if (context.maxResolution > currentRead &&
                    context.minResolution < context.globalPointer) {
                return fillFromResolved(latestTokenValue, context);
            }

            // If the start is available in the resolved queue,
            // use it.
            if (context.minResolution <= context.globalPointer) {
                fillFromResolved(maxGlobal, context);
            }

            // Now we calculate the next entry to read.
            // If we have a backpointer, we'll use that for our next read.
            if (!runtime.backpointersDisabled &&
                    currentEntry.hasBackpointer(context.id)) {
                log.trace("Read_Fill_Queue[{}] Backpointer {}->{}", this,
                        currentRead, currentEntry.getBackpointer(context.id));
                currentRead = currentEntry.getBackpointer(context.id);
            }
            // Otherwise, our next read is the previous entry.
            else {
                currentRead = currentRead - 1L;
            }

            // If the next read is before or equal to the max resolved
            // we need to stop.
            if (context.maxResolution >= currentRead) {
                break;
            }

        }

        log.debug("Read_Fill_Queue[{}] Filled CP queue with {}", this, context.readCpQueue);
        log.debug("Read_Fill_Queue[{}] Filled queue with {}", this, context.readQueue);
        return ! context.readCpQueue.isEmpty() || !context.readQueue.isEmpty();
    }

    private void examineCheckpointRecord(final QueuedStreamContext context,
                                         ILogData currentEntry,
                                         boolean considerCheckpoint, long currentRead) {
        CheckpointEntry.CheckpointEntryType cpType = currentEntry.getCheckpointType();
        UUID cpID = currentEntry.getCheckpointID();

        if (context.checkpointSuccessID == null &&
                cpType == CheckpointEntry.CheckpointEntryType.END) {
            CheckpointEntry cpEntry = (CheckpointEntry) currentEntry.getPayload(runtime);
            log.trace("Checkpoint: address {} found END, id {} author {}",
                    currentRead, cpEntry.getCheckpointID(), cpEntry.getCheckpointAuthorID());
            if (considerCheckpoint) {
                context.checkpointSuccessID = cpEntry.getCheckpointID();
                context.checkpointSuccessNumEntries = 1L;
                context.checkpointSuccessBytes = (long) currentEntry.getSizeEstimate();
                context.checkpointSuccessEndAddr = currentRead;
            }
        }
        if (context.checkpointSuccessID != null &&
                context.checkpointSuccessID.equals(cpID)) {
            CheckpointEntry cpEntry = (CheckpointEntry) currentEntry.getPayload(runtime);
            log.trace("Checkpoint: address {} type {} id {} author {}",
                    currentRead, cpEntry.getCpType(),
                    cpEntry.getCheckpointID(), cpEntry.getCheckpointAuthorID());
            if (considerCheckpoint) {
                context.readCpQueue.add(currentEntry.getGlobalAddress());
                context.checkpointSuccessNumEntries++;
                context.checkpointSuccessBytes += cpEntry.getSmrEntriesBytes();
                if (cpEntry.getCpType().equals(CheckpointEntry.CheckpointEntryType.START)) {
                    long cpStartAddr;
                    if (cpEntry.getDict().get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS) != null) {
                        cpStartAddr = Long.decode(cpEntry.getDict()
                                .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS));
                    } else {
                        cpStartAddr = currentRead;
                    }
                    context.checkpointSuccessStartAddr = cpStartAddr;
                    log.trace("Checkpoint: halt backpointer fill at address {} type {} id {} author {}",
                            cpStartAddr, cpEntry.getCpType(),
                            cpEntry.getCheckpointID(), cpEntry.getCheckpointAuthorID());
                    return;
                }
            }
        } else {
            log.trace("Checkpoint: skip address {} type {} id {}", currentRead, cpType, cpID);
        }
    }
}
