package org.corfudb.logreplication;

import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.send.LogReplicationError;

import java.util.List;
import java.util.UUID;


/**
 * This Interface comprises Data Path send operations.
 *
 * Application is expected
 */
public interface DataSender {

    /**
     * Application callback on next available message for transmission to remote site during snapshot sync.
     *
     * @param message DataMessage representing the data to send across sites.
     * @param snapshotSyncId snapshot sync event identifier in progress.
     *
     * @return False, in the event of errors. True, otherwise.
     */
    boolean send(DataMessage message, UUID snapshotSyncId);

    /**
     * Application callback on next available messages for transmission to remote site during snapshot sync.
     *
     * @param messages list o DataMessage representing the data to send across sites.
     * @param snapshotSyncId snapshot sync event identifier in progress.
     *
     * @return False, in the event of errors. True, otherwise.
     */
    boolean send(List<DataMessage> messages, UUID snapshotSyncId);

    /**
     * Call to the application indicating the full sync of streams on the given snapshot has completed.
     * Applications can react on completeness according to their protocol.
     *
     * @param snapshotSyncId  event identifier of the completed snapshot sync.
     */
    boolean complete(UUID snapshotSyncId);
    // TODO (Anny) Optimize? Resend complete message n times, before failing?...

    /**
     * Application callback on next available message for transmission to remote site during log entry sync.
     *
     * @param message DataMessage representing the data to send across sites.
     * @return
     */
    boolean send(DataMessage message);

    /**
     * Application callback on next available messages for transmission to remote site during log entry sync.
     *
     * @param messages list of DataMessage representing the data to send across sites.
     * @return
     */
    boolean send(List<DataMessage> messages);

    /**
     * Application callback on error during snapshot sync.
     *
     * @param error log replication error
     * @param snapshotSyncId Identifier of the event that was interrupted on an error
     */
    void onError(LogReplicationError error, UUID snapshotSyncId);

    /**
     * Application callback on error during snapshot sync.
     *
     * @param error log replication error
     */
    void onError(LogReplicationError error);

}
