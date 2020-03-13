package org.corfudb.logreplication.fsm;

import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationError;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

import java.util.List;

/**
 *  Empty Implementation of Snapshot Listener - used for state machine transition testing (no logic)
 */
public class EmptyDataSender implements DataSender {

    @Override
    public boolean send(LogReplicationEntry message) { return true; }

    @Override
    public boolean send(List<LogReplicationEntry> messages) { return true; }

    @Override
    public void onError(LogReplicationError error) {}
}
