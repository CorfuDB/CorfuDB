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

    /*
     -------------------- SNAPSHOT SYNC METHODS ----------------------
     */

    /**
     * Application callback on next available message for transmission to remote site during snapshot sync.
     *
     * @param message DataMessage representing the data to send across sites.
     * @param snapshotSyncId snapshot sync event identifier in progress.
     * @param completed indicates this is the last message
     *
     * @return False, in the event of errors. True, otherwise.
     */
    boolean send(DataMessage message, UUID snapshotSyncId, boolean completed);

    /**
     * Application callback on next available messages for transmission to remote site during snapshot sync.
     *
     * @param messages list o DataMessage representing the data to send across sites.
     * @param snapshotSyncId snapshot sync event identifier in progress.
     * @param completed indicates this is the last message
     *
     * @return False, in the event of errors. True, otherwise.
     */
    boolean send(List<DataMessage> messages, UUID snapshotSyncId, boolean completed);

    /**
     * Application callback on error during snapshot sync.
     *
     * @param error log replication error
     * @param snapshotSyncId Identifier of the event that was interrupted on an error
     */
    void onError(LogReplicationError error, UUID snapshotSyncId);

    /*
     -------------------- LOG ENTRY SYNC METHODS ----------------------
     */
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
     */
    void onError(LogReplicationError error);
}
