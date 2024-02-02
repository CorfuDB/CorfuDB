package org.corfudb.runtime;

import org.corfudb.runtime.LogReplication.ReplicationEvent.ReplicationEventType;
import org.corfudb.runtime.collections.ScopedTransaction;
import org.corfudb.runtime.collections.TxnContext;
import static org.corfudb.runtime.Queue.RoutingTableEntryMsg;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.CancellationException;

public interface LRFullStateReplicationContext {
    /**
     * LR starts this transaction and the callback must use the same to do its full table scans
     */
    TxnContext getTxn();

    /**
     * This can be used to determine if a snapshot was already set for this context
     *
     * @return the snapshot if set, null if not
     */
    ScopedTransaction getSnapshot();

    /**
     * Caller needs to set this value in with the snapshot of all the tables to be sent
     */
    void setSnapshot(ScopedTransaction snapshot);

    /**
     * Returns destination site ID.
     * Data is transmitted from this site.
     */
    String getDestinationSiteId();

    /**
     * Returns unique ID for the full state sync request.
     */
    UUID getRequestId();

    /**
     * Returns reason for full state sync request.
     */
    @Nullable
    ReplicationEventType getReason();

    /**
     * Transmits one message for full sync.
     */
    void transmit(RoutingTableEntryMsg message) throws CancellationException;

    /**
     * Transmits one message for full sync.
     *
     * @param message  message to transmit.
     * @param progress indicates progress of transmission, value between 0 and 100.
     */
    void transmit(RoutingTableEntryMsg message, int progress) throws CancellationException;

    /**
     * Indicates that all data was transmitted from application to client.
     */
    void markCompleted() throws CancellationException;

    /**
     * Cancels this full sync replication.
     * Application cannot continue and full sync has to be restarted.
     */
    void cancel();
}
