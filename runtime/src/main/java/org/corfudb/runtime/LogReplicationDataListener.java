package org.corfudb.runtime;

import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.LogReplication.ReplicationStatusVal;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.TableSchema;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE;

/**
 * This is the interface that a client must subscribe to if it needs to observe and bifurcate the data updates received
 * through Log Replication based on Log Entry and Snapshot Sync.
 *
 *
 * This interface sees ordered updates from :
 * 1. client-streams from client-Namespace, and,
 * 2. LrStatusTable from corfuSystem-Namespace.
 *
 * The client implementing this interface will only observe the data updates from client streams
 */
public abstract class LogReplicationDataListener implements StreamListener {

    private final AtomicBoolean snapshotSyncInProgress = new AtomicBoolean(false);

    /**
     * A corfu update can/may have multiple updates belonging to different streams.
     * This callback will return those updates as a list grouped by their Stream UUIDs.
     *
     * Note: there is no order guarantee within the transaction boundaries.
     *
     * @param results is a map of stream UUID -> list of entries of this stream.
     */
    public final void onNext(CorfuStreamEntries results) {

        Map<TableSchema, List<CorfuStreamEntry>> entries = results.getEntries();

        for (TableSchema tableSchema : entries.keySet()) {
            if (Objects.equals(tableSchema.getTableName(), REPLICATION_STATUS_TABLE)) {
                for (CorfuStreamEntry entry : entries.get(tableSchema)) {
                    ReplicationStatusVal status = (ReplicationStatusVal) entry.getPayload();
                    if (!status.getDataConsistent()) {
                        snapshotSyncInProgress.set(true);
                        onSnapshotSyncStart();
                    } else if (snapshotSyncInProgress.get()) {
                        snapshotSyncInProgress.set(false);
                        onSnapshotSyncComplete();
                    }
                }
                // Data updates will not be received in the same transaction as Replication Status updates
                return;
            }
        }

        // Updates from the data tables
        if (snapshotSyncInProgress.get()) {
            processUpdateInSnapshotSync(results);
        } else {
            processUpdateInLogEntrySync(results);
        }
    }

    //      -------- Methods to be implemented on the client/application  ---------------

    /**
     * Invoked when a snapshot sync start has been detected.
     */
    protected abstract void onSnapshotSyncStart();

    /**
     * Invoked when an ongoing snapshot sync completes
     */
    protected abstract void onSnapshotSyncComplete();

    /**
     * Invoked when data updates are received during a snapshot sync.  These updates will be the writes
     * received as part of the snapshot sync
     * @param results Entries received in a single transaction as part of a snapshot sync
     */
    protected abstract void processUpdateInSnapshotSync(CorfuStreamEntries results);

    /**
     * Invoked when data updates are received as part of a LogEntry Sync.
     * @param results Entries received in a single transaction as part of a log entry sync
     */
    protected abstract void processUpdateInLogEntrySync(CorfuStreamEntries results);

    /**
     * Invoked by the Corfu runtime when this listener is being subscribed for receiving updates.  This method should
     * start a read-only transaction invoking getTable() on all application tables which will be
     * modified in this listener and the replicated LR tables and return the commit timestamp of the transaction.
     * The timestamp will be the max of the tails of all tables on which getTable() was performed.
     * @return Timestamp commit timestamp of the read-only transaction
     */
    protected abstract Timestamp performMultiTableReads();

    /**
     * Invoked by the Corfu runtime when this listener is being subscribed for receiving updates.  This method merges
     * (constructs a baseline) of the replicated LR tables as seen at the given timestamp.
     * @param timestamp timestamp at which the replicated tables must be read to form the baseline
     */
    protected abstract void mergeTableOnSubscription(Timestamp timestamp);

    /**
     * Callback to indicate that an error or exception has occurred while streaming or that the stream is
     * shutting down. Some exceptions can be handled by restarting the stream (TrimmedException) while
     * some errors (SystemUnavailableError) are unrecoverable.
     * To be implemented on the client/application
     * @param throwable
     */
    public abstract void onError(Throwable throwable);
}
