package org.corfudb.infrastructure.logreplication.replication.fsm;

import org.corfudb.infrastructure.logreplication.replication.send.logreader.TxStreamReader;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.UUID;

/**
 * Test Implementation of Log Entry Reader
 */
public class TestLogEntryReader extends TxStreamReader {
    @Override
    public LogReplicationEntry read(UUID logEntryRequestId) {
        return null;
    }

    @Override
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
        // Read everything from start
    }
}
