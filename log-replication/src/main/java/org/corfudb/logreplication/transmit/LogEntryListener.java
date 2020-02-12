package org.corfudb.logreplication.transmit;

import org.corfudb.logreplication.LogReplicationError;
import org.corfudb.logreplication.message.DataMessage;

import java.util.List;

/**
 * An interface for a Log Entry Listener.
 *
 * The transmission of data across sites is handled by the application itself. Applications must implement this
 * interface defining their own communication channels. They will receive consecutive TxMessages for every
 * incremental (delta) update through this interface.
 */
public interface LogEntryListener {

    /**
     * Application callback on next available message for transmission to remote site during log entry sync.
     *
     * @param message DataMessage representing the data to transmit across sites.
     * @return
     */
    boolean onNext(DataMessage message);

    /**
     * Application callback on next available messages for transmission to remote site during log entry sync.
     *
     * @param messages list of DataMessage representing the data to transmit across sites.
     * @return
     */
    boolean onNext(List<DataMessage> messages);

    /**
     * Application callback on error during log entry sync.
     *
     * @param error log replication error
     */
    void onError(LogReplicationError error);

}
