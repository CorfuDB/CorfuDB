package org.corfudb.logreplication.infrastructure;

import org.corfudb.logreplication.DataControl;
import org.corfudb.logreplication.DataSender;
import org.corfudb.logreplication.SourceManager;
import org.corfudb.logreplication.fsm.LogReplicationConfig;

public class CorfuReplicationManager {

    enum LogReplicationNegotiationResult {
        SNAPSHOT_SYNC,
        LOG_ENTRY_SYNC,
        LEADERSHIP_LOST,
        CONNECTION_LOST,
        UNKNOWN
    }

    /**
     * Once determined this is a Lead Sender (on primary site), start log replication.
     */
    public void startLogReplication(String remoteEndpoint) {
        // Initiate Log Replication Negotiation Protocol 
        LogReplicationNegotiationResult negotiationResult = startNegotiation(remoteEndpoint);

        switch (negotiationResult) {
            case SNAPSHOT_SYNC:
                startSnapshotSync();
                break;
            case LOG_ENTRY_SYNC:
                startLogEntrySync();
                break;
            case LEADERSHIP_LOST:
                break;
            case CONNECTION_LOST:
                break;
            case UNKNOWN:
                break;
        }
    }


    private void startSnapshotSync() {
        // Start Log Replication
        //Runtime
        //      DataSender
        //DataControl
        //LogReplicationConfig
        //SourceManager sourceManager = new SourceManager();
        //sourceManager.
    }

    private void startLogEntrySync() {
    }


    /**
     * Once determined this is a Lead Receiver (on standby site), start log receivers.
     */
    public void startLogReceive() {

    }

    private LogReplicationNegotiationResult startNegotiation(String remoteEndpoint) {
        return LogReplicationNegotiationResult.UNKNOWN;
    }


}
