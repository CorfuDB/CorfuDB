/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.runtime.view.stream;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;

/**
 * Created by Konstantin Spirov on 2/20/2017.
 */
@Slf4j
public class QuorumStreamView extends BackpointerStreamView {
    /**
     * Create a new backpointer stream view.
     *
     * @param runtime  The runtime to use for accessing the log.
     * @param streamID The ID of the stream to view.
     */
    public QuorumStreamView(CorfuRuntime runtime, UUID streamID) {
        super(runtime, streamID);
    }


    private static ThreadLocal<Integer> recoveryModeRankThreadLocal = new ThreadLocal();

    /**
     * @return Whether the quorum replication view should try a recovery mode with a given rank, otherwise 0.
     */
    public static int getRecoveryModeRankLocal() {
       Integer res = recoveryModeRankThreadLocal.get();
       if (res==null) {
           return 0;
       }
       return res;
    }


    /**
     * {@inheritDoc}
     *
     * In quorum stream view it is possible the offset to change, so we are returning the last token
     * from the sequencer for this context id.
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

        boolean failover = runtime.getSequencerView().isFailOverSequencer();
        int rank = 0;
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

            // >0 means a recovery mode
            recoveryModeRankThreadLocal.set(rank);

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
                                1);
            } finally {
                recoveryModeRankThreadLocal.set(null);
            }
            rank++;
        }
    }

}
