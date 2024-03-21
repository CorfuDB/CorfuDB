package org.corfudb.infrastructure.logreplication.replication.fsm;

import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsLogEntryReader;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.view.Address;

import java.util.UUID;

/**
 * Test Implementation of Log Entry Reader
 */
public class TestLogEntryReader implements LogEntryReader {

    @Override
    public LogReplication.LogReplicationEntryMsg read(UUID logEntryRequestId) {
        return null;
    }

    @Override
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
        // Read everything from start
    }

    @Override
    public void setTopologyConfigId(long siteConfigID) {

    }

    @Override
    public boolean hasMessageExceededSize() {
        return false;
    }

    @Override
    public StreamsLogEntryReader.StreamIteratorMetadata getCurrentProcessedEntryMetadata() {
        return new StreamsLogEntryReader.StreamIteratorMetadata(Address.NON_ADDRESS, false);
    }
}
