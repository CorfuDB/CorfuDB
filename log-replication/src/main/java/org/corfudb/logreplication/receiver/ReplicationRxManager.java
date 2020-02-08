package org.corfudb.logreplication.receiver;

import org.corfudb.logreplication.transmitter.DataMessage;
import org.corfudb.runtime.CorfuRuntime;

import java.util.List;

/**
 * A class that represents the Replication Receiver Manager.
 * This is the entry point for destination site.
 */
public class ReplicationRxManager {
    private CorfuRuntime runtime;

    /**
     * Constructor
     *
     * @param rt Corfu Runtime
     */
    public ReplicationRxManager(CorfuRuntime rt) {
        this.runtime = rt;
    }

    /**
     * Apply message to log.
     *
     * @param message
     */
    public void apply(DataMessage message) {
        // Buffer data (out of order) and apply
    }

    /**
     * Apply messages to log.
     *
     * @param messages
     */
    public void apply(List<DataMessage> messages) {
        // Buffer data (out of order) and apply
    }

    /**
     * Signal the manager a snapshot sync is about to start. This is required to reset previous states.
     */
    public void startSnapshot() {
        // Signal start of snapshot sync to the receiver, so data can be cleared.
    }
}
