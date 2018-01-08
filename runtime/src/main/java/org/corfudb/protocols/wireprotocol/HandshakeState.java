package org.corfudb.protocols.wireprotocol;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by amartinezman on 12/20/17.
 */
public class HandshakeState {

    /** Indicates if the handshake has failed.
     * - If it fails because of validation, handshakeComplete = true.
     * - If it fails due to timeout, handshakeComplete = False.
     * */
    private final AtomicBoolean handshakeFailed;
    /** Indicates if the handshake has completed regardless of the result of validation.*/
    private final AtomicBoolean handshakeComplete;

    /**
     * Keeps state of the handshake, initiated between client and server.
     */
    public HandshakeState() {
        this.handshakeFailed = new AtomicBoolean(false);
        this.handshakeComplete = new AtomicBoolean(false);
    }

    /**
     * Set Handshake State
     * @param failed indicates if handshake failed
     * @param complete indicates if handshake completed between client/server, i.e., did not timeout.
     */
    public void set(boolean failed, boolean complete){
        this.handshakeFailed.set(failed);
        this.handshakeComplete.set(complete);
    }

    /**
     * Get Handshake Failure State
     * @return true, if failed
     *         false, otherwise.
     */
    public boolean failed(){
        return this.handshakeFailed.get();
    }

    /**
     * Get Handshake Completeness State
     * @return true, if handshake completed.
     *         false, if handshake never concluded.
     */
    public boolean completed(){
        return this.handshakeComplete.get();
    }
}
