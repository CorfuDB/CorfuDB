package org.corfudb.logreplication.fsm;

import org.corfudb.logreplication.LogReplicationError;
import org.corfudb.logreplication.transmitter.SnapshotListener;
import org.corfudb.logreplication.transmitter.DataMessage;

import java.util.List;
import java.util.UUID;

/**
 *  Empty Implementation of Snapshot Listener - used for state machine transition testing (no logic)
 */
public class EmptySnapshotListener implements SnapshotListener {
    @Override
    public boolean onNext(DataMessage message, UUID snapshotSyncId) {
        return true;
    }

    @Override
    public boolean onNext(List<DataMessage> messages, UUID snapshotSyncId) {
        return true;
    }

    @Override
    public boolean complete(UUID snapshotSyncId) { return true; }

    @Override
    public void onError(LogReplicationError error, UUID snapshotSyncId) {}
}
