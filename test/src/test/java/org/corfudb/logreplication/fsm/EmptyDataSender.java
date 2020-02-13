package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.DataSender;
import org.corfudb.logreplication.transmit.LogReplicationError;
import org.corfudb.logreplication.message.DataMessage;

import java.util.List;
import java.util.UUID;

/**
 *  Empty Implementation of Snapshot Listener - used for state machine transition testing (no logic)
 */
public class EmptyDataSender implements DataSender {
    @Override
    public boolean onNext(DataMessage message, UUID snapshotSyncId) {
        return true;
    }

    @Override
    public boolean onNext(List<DataMessage> messages, UUID snapshotSyncId) {
        return true;
    }

    @Override
    public boolean onNext(DataMessage message) { return true; }

    @Override
    public boolean onNext(List<DataMessage> messages) { return true; }

    @Override
    public boolean complete(UUID snapshotSyncId) { return true; }

    @Override
    public void onError(LogReplicationError error, UUID snapshotSyncId) {}
}
