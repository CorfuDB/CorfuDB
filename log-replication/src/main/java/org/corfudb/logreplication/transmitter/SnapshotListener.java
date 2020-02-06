package org.corfudb.logreplication.transmitter;

import org.corfudb.logreplication.LogReplicationError;

import java.util.List;
import java.util.UUID;


public interface SnapshotListener {

    /**
     * Application callback on next available message for transmission to remote site during snapshot sync.
     *
     * @param message TxMessage representing the data to transmit across sites.
     * @param snapshotSyncId snapshot sync event identifier in progress.
     *
     * @return False, in the event of errors. True, otherwise.
     */
    // TODO: Do we really want to block?
    boolean onNext(TxMessage message, UUID snapshotSyncId);

    /**
     * Application callback on next available messages for transmission to remote site during snapshot sync.
     *
     * @param messages list of TxMessage representing the data to transmit across sites.
     * @param snapshotSyncId snapshot sync event identifier in progress.
     *
     * @return False, in the event of errors. True, otherwise.
     */
    boolean onNext(List<TxMessage> messages, UUID snapshotSyncId);

    /**
     * Call to the application indicating the full sync of streams on the given snapshot has completed.
     * Applications can react on completeness according to their protocol.
     *
     * @param snapshotSyncId  event identifier of the completed snapshot sync.
     */
    void complete(UUID snapshotSyncId);

    /**
     * Application callback on error during snapshot sync.
     *
     * @param error log replication error
     * @param snapshotSyncId Identifier of the event that was interrupted on an error
     */
    void onError(LogReplicationError error, UUID snapshotSyncId);
}
