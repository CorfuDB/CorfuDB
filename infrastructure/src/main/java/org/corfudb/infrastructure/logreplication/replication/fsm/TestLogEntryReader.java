package org.corfudb.infrastructure.logreplication.replication.fsm;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsLogEntryReader;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.view.Address;

import java.util.UUID;

/**
 * Test Implementation of Log Entry Reader
 */
public class TestLogEntryReader extends LogEntryReader {

    public TestLogEntryReader(LogReplicationSession session, LogReplicationContext replicationContext) {
        super(session, replicationContext);
    }

    @Override
    public LogReplicationEntryMsg read(UUID logEntryRequestId) {
        return null;
    }

    @Override
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
        // Read everything from start
    }

    @Override
    public StreamsLogEntryReader.StreamIteratorMetadata getCurrentProcessedEntryMetadata() {
        return new StreamsLogEntryReader.StreamIteratorMetadata(Address.NON_ADDRESS, false);
    }
}
