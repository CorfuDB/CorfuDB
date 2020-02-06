package org.corfudb.logreplication.transmitter;

import org.corfudb.logreplication.LogReplicationError;

import java.util.List;
import java.util.UUID;

public interface SnapshotListener {

    boolean onNext(TxMessage message, UUID snapshotSyncId);

    boolean onNext(List<TxMessage> messages, UUID snapshotSyncId);

    /**
     * Call to the application on transmit complete.
     */
    void complete(UUID snapshotSyncId);

    // Define error codes to pass
    void onError(LogReplicationError error, UUID snapshotSyncId);
}
