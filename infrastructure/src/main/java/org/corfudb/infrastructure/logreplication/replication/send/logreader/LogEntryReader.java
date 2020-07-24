package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.UUID;

/**
 * An Interface for Log Entry Reader
 *
 * A log entry logreader provides the functionality for reading incremental updates from Corfu.
 */
public interface LogEntryReader {

    /**
     * Read a Log Entry.
     *
     * @param logEntryRequestId unique identifier of log entry sync request.
     *
     * @return a log replication entry.
     */
    LogReplicationEntry read(UUID logEntryRequestId);

    void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp);

    // Set current topologyConfigId that will be used to construct messages.
    void setTopologyConfigId(long topologyConfigId);

    // If the transaction log contains both replicated streams and other streams,
    // we treat it as nosieData.
    // If the log data size is bigger than the max msg size supported, we set hasNoiseData too.
    // When haNoiseData, a log replication exception will be thrown and triggers
    // to stop the log replication state machine.
    boolean hasNoiseData();
}
