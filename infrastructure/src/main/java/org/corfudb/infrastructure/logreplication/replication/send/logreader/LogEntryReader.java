package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.runtime.LogReplication;

import java.util.UUID;

/**
 * An Interface for Log Entry Reader
 *
 * A log entry reader provides the functionality for reading incremental updates from Corfu.
 */
public interface LogEntryReader {

    /**
     * Read a Log Entry.
     *
     * @param logEntryRequestId unique identifier of log entry sync request.
     *
     * @return a log replication entry.
     */
    LogReplication.LogReplicationEntryMsg read(UUID logEntryRequestId);

    void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp);

    void setTopologyConfigId(long topologyConfigId);

    boolean hasMessageExceededSize();

    StreamsLogEntryReader.StreamIteratorMetadata getCurrentProcessedEntryMetadata();
}
