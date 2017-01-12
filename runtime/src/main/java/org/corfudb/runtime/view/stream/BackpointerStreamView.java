package org.corfudb.runtime.view.stream;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Address;

import java.util.*;
import java.util.function.Function;

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

    /**
     * The number of retries before attempting a hole fill.
     * TODO: this constant should come from the runtime.
     */
    final int NUM_RETRIES = 3;

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
                        .write(tokenResponse.getToken(),
                                Collections.singleton(ID),
                                object,
                                tokenResponse.getBackpointerMap(),
                                tokenResponse.getStreamAddresses());
                // The write completed successfully, so we return this
                // address to the client.
                return tokenResponse.getToken();
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
                             1, true, false);
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

    /**
     * {@inheritDoc}
     *
     * In the backpointer based implementation, we indicate we may have
     * entries available if the read queue contains entries to read -or-
     * if the next token is greater than our log pointer.
     */
    @Override
    public boolean getHasNext(QueuedStreamContext context) {
        return  context.readQueue.isEmpty() ||
                runtime.getSequencerView()
                .nextToken(Collections.singleton(context.id), 0).getToken()
                        > context.globalPointer;
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
    protected boolean fillReadQueue(final long maxGlobal,
                                 final QueuedStreamContext context) {
        // The maximum address we will fill to.
        final long maxAddress =
                Long.min(maxGlobal, context.maxGlobalAddress);

        // If the maximum address is less than the current pointer,
        // we return since there is nothing left to do.
        if (context.globalPointer > maxAddress) {
            return false;
        }

        // First, we fetch the current token (backpointer) from the sequencer.
        final long latestToken = runtime.getSequencerView()
                .nextToken(Collections.singleton(context.id), 0)
                .getToken();

        // If the backpointer was unwritten, return, there is nothing to do
        if (latestToken == Address.NEVER_READ) {
            return false;
        }

        // Now we start traversing backpointers, if they are available. We
        // start at the latest token and go backward, until we reach the
        // log pointer. For each address which is less than
        // maxGlobalAddress, we insert it into the read queue.
        long currentRead = latestToken;

        while (currentRead > context.globalPointer &&
                currentRead != Address.NEVER_READ) {
            // Read the entry in question.
            ILogData currentEntry =
                    runtime.getAddressSpaceView().read(currentRead);

            // If the current entry is unwritten, we need to fill it,
            // otherwise we cannot resolve the stream.
            if (currentEntry.getType() == DataType.EMPTY) {

                // We'll retry the read a few times, we should only need
                // to fill if a client has actually failed, which should
                // be a relatively rare event.

                for (int i = 0; i < NUM_RETRIES; i++) {
                    currentEntry =
                            runtime.getAddressSpaceView().read(currentRead);
                    if (currentEntry.getType() != DataType.EMPTY) {
                        break;
                    }
                    // Wait 1 << i ms (exp. backoff) before retrying again.
                    try {
                        Thread.sleep(1 << i);
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                }

                // If we STILL don't have the data, now we need to do a hole
                // fill.
                if (currentEntry.getType() == DataType.EMPTY) {
                    try {
                        runtime.getAddressSpaceView().fillHole(currentRead);
                        // If we reached here, our hole fill was successful.
                    } catch (OverwriteException oe) {
                        // If we reached here, this means the remote client
                        // must have successfully completed the write and
                        // we can continue.
                    }
                }

                // At this point the hole is filled or has the data and we
                // can continue.
            }

            // If the entry contains this context's stream,
            // and it is less than max read, we add it to the read queue.
            if (currentEntry.containsStream(context.id) &&
                    currentRead <= maxAddress) {
                context.readQueue.add(currentRead);
            }

            // Now we calculate the next entry to read.
            // If we have a backpointer, we'll use that for our next read.
            if (!runtime.backpointersDisabled &&
                    currentEntry.hasBackpointer(context.id)) {
                currentRead = currentEntry.getBackpointer(context.id);
            }
            // Otherwise, our next read is the previous entry.
            else {
                currentRead = currentRead - 1L;
            }
        }

        return !context.readQueue.isEmpty();
    }
}
