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
 * This is the callback interface that any client subscribing to updates across the LR Status Table
 * (ReplicationStatus) and application-specific tables must implement.
 *
 * This interface sees ordered updates from :
 * 1. client-streams from client-Namespace, and,
 * 2. LrStatusTable from corfuSystem-Namespace.
 */
public abstract class LRMultiNamespaceListener implements StreamListener {

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
     * Callback invoked when a snapshot sync start has been detected.
     */
    protected abstract void onSnapshotSyncStart();

    /**
     * Callback invoked when an ongoing snapshot sync completes
     */
    protected abstract void onSnapshotSyncComplete();

    /**
     * Callback invoked when data updates are received during a snapshot sync.  These updates will be the writes
     * received as part of the snapshot sync
     * @param results Entries received in a single transaction as part of a snapshot sync
     */
    protected abstract void processUpdateInSnapshotSync(CorfuStreamEntries results);

    /**
     * Callback invoked when data updates are received as part of a LogEntry Sync.
     * @param results Entries received in a single transaction as part of a log entry sync
     */
    protected abstract void processUpdateInLogEntrySync(CorfuStreamEntries results);

    /**
     *
     */
    protected abstract Timestamp performMultiTableReads();

    /**
     *
     */
    protected abstract void mergeTableOnInitialSubscription(Timestamp timestamp);

    /**
     * Callback to indicate that an error or exception has occurred while streaming or that the stream is
     * shutting down. Some exceptions can be handled by restarting the stream (TrimmedException) while
     * some errors (SystemUnavailableError) are unrecoverable.
     * To be implemented on the client/application
     * @param throwable
     */
    public abstract void onError(Throwable throwable);
}
