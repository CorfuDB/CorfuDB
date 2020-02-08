package org.corfudb.logreplication.receiver;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.transmitter.DataMessage;
import org.corfudb.runtime.CorfuRuntime;

import java.util.List;

/**
 * A class that represents the Replication Receiver Manager.
 * This is the entry point for destination site.
 */
@Slf4j
public class ReplicationRxManager {

    // TODO: THIS CLASS NEEDS TO BE WORKED

    private CorfuRuntime runtime;
    private StreamsSnapshotWriter snapshotWriter;

    /**
     * Constructor
     *
     * @param rt Corfu Runtime
     */
    public ReplicationRxManager(CorfuRuntime rt, LogReplicationConfig config) {
        this.runtime = rt;
        this.snapshotWriter = new StreamsSnapshotWriter(rt, config);
    }

    /**
     * Apply message to log.
     *
     * @param message
     */
    public void apply(DataMessage message) {
        // @maxi, how do we distinguish log entry apply from snapshot apply?
        // Buffer data (out of order) and apply
        try {
            this.snapshotWriter.apply(message);
        } catch (Exception e) {

        }

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
        // Metadata to clear?
    }
}
