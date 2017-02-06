package org.corfudb.runtime.view.stream;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.ReplexOverwriteException;
import org.corfudb.runtime.view.Address;

import java.util.Collections;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;

/** A view of a stream implemented using Replex.
 *
 *
 * TODO: This implementation does not implement bulk reads anymore. This will
 * be addressed once the address space implementation is refactored into an
 * address space view and a replex (branch?) view.
 *
 * All method calls of this class are thread-safe.
 *
 * Created by mwei on 1/5/17.
 */
@Slf4j
public class ReplexStreamView extends
        AbstractContextStreamView<ReplexStreamView.ReplexStreamContext> {

    /**
     * The number of retries before attempting a hole fill.
     * TODO: this constant should come from the runtime.
     */
    final int NUM_RETRIES = 3;

    /** Create a new replex stream view.
     *
     * @param runtime   The runtime to use for accessing the log.
     * @param streamID  The ID of the stream to view.
     */
    public ReplexStreamView(final CorfuRuntime runtime,
                                 final UUID streamID) {
        super(runtime, streamID, ReplexStreamContext::new);
    }

    /** {@inheritDoc}
     *
     * In Replex, stream addresses are returned by the sequencer. When an
     * overwrite error occurs, we inform the sequencer whether or not we want
     * a new stream address (token) ONLY or both global and stream addresses.
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
                    return Address.ABORTED;
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
            }
            catch (OverwriteException oe) {
                log.trace("Overwrite occurred at {}", tokenResponse);
                // We got overwritten, so we call the deacquisition callback
                // to inform the client we didn't get the address.
                if (deacquisitionCallback != null) {
                    if (!deacquisitionCallback.apply(tokenResponse)) {
                        log.debug("Deacquisition requested abort");
                        return Address.ABORTED;
                    }
                }
                // Request a new token, informing the sequencer
                // of the overwrite.
                tokenResponse = runtime.getSequencerView()
                        .nextToken(Collections.singleton(ID),
                              1,
                                // If this is a normal overwrite
                                !(oe instanceof ReplexOverwriteException),
                                // If this is a Replex overwrite
                                oe instanceof ReplexOverwriteException);
            }
        }
    }

    /** Update the known maximum stream address, given the context.
     *
     * @param context   The context to use.
     */
    private void updateKnownMax(final ReplexStreamContext context) {
        context.knownStreamMax = runtime.getSequencerView()
                .nextToken(Collections.singleton(context.id), 0)
                .getStreamAddresses().get(context.id);
    }

    /** {@inheritDoc}
     *
     * In Replex, we use stream addresses. We can't determine
     * the global address until we perform the read.
     *
     * */
    @Override
    protected LogData getNextEntry(final ReplexStreamContext context,
                                   long maxGlobal) {
        // If we are at (or greater than, which shouldn't occur...) the known
        // stream maximum, we need to check if theres a new stream maximum.
        if (context.streamPointer >= context.knownStreamMax) {
            updateKnownMax(context);
            // There are no new entries, so we return null
            if (context.streamPointer >= context.knownStreamMax)
            {
                return null;
            }
        }

        // Read until we have exceeded the known stream maximum
        while (context.streamPointer < context.knownStreamMax) {
            // The address we are reading from
            final long thisRead = context.streamPointer + 1;

            // Perform the read using replex.
            LogData ld = runtime.getAddressSpaceView()
                    .read(context.id, thisRead, 1L)
                    .get(context.streamPointer);

            // Do we have data? If not, retry the given number of times before
            // attempting a hole fill.
            for (int i = 0; i < NUM_RETRIES; i++) {
                ld = runtime.getAddressSpaceView()
                        .read(context.id, thisRead, 1L)
                        .get(thisRead);
                if (ld.getType() != DataType.EMPTY) {
                    break;
                }
            }

            // If after we retry the data is still empty, let's hole fill.
            while (ld.getType() == DataType.EMPTY) {
                try {
                    runtime.getAddressSpaceView()
                            .fillStreamHole(context.id, thisRead);
                } catch (OverwriteException oe) {
                    // If we're overwritten while hole filling, that's okay
                    // since we're going to re-read anyway
                }
                ld = runtime.getAddressSpaceView()
                        .read(ID, thisRead, 1L)
                        .get(thisRead);
            }

            // Check if we are within maxGlobal.
            if (ld.getType() == DataType.DATA) {
                if (ld.getGlobalAddress() > maxGlobal) {
                    // We exceed the max global.
                    return null;
                }
                context.streamPointer++;
                return ld;
            }

            // If this entry was a hole, or not data, update
            // the stream max to make sure we captured any
            // entries that might have been added.
            context.streamPointer++;
            updateKnownMax(context);
        }
        // Return null, as there were no entries which were data
        // which belonged to this stream.
        return null;
    }

    /**
     * {@inheritDoc}
     *
     * Just like the backpointer based implementation, we indicate we may have
     * entries available if the read queue contains entries to read -or-
     * if the next token is greater than our log pointer.
     */
    @Override
    public boolean getHasNext(ReplexStreamContext context) {
        return  context.knownStreamMax > context.streamPointer ||
                runtime.getSequencerView()
                        .nextToken(Collections.singleton(context.id),
                                0).getToken()
                        > context.globalPointer;
    }

    /** {@inheritDoc}
     *
     * For a replex stream context, we include just a stream pointer, which
     * uses the stream address.
     */
    @ToString
    static class ReplexStreamContext extends AbstractStreamContext {

        /** A pointer to the current position in the stream which
         * uses the stream address.
         */
        long streamPointer;

        /** The largest known stream address we know was issued. */
        long knownStreamMax;

        /** Create a new stream context with the given ID and maximum address
         * to read to.
         * @param id                  The ID of the stream to read from
         * @param maxGlobalAddress    The maximum address for the context.
         */
        public ReplexStreamContext(UUID id, long maxGlobalAddress) {
            super(id, maxGlobalAddress);
            streamPointer = Address.NEVER_READ;
            knownStreamMax = Address.NEVER_READ;
        }
    }
}
