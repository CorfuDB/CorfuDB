package org.corfudb.runtime.collections;

import lombok.Setter;
import org.corfudb.runtime.LogReplication.ReplicationStatusVal;
import org.corfudb.runtime.view.ObjectsView;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This is the callback interface that any client subscribing to updates across the LR Status Table
 * (ReplicationStatus) and application-specific tables must implement.
 *
 * This interface sees ordered updates from :
 * 1. client-streams from client-Namespace, and,
 * 2. LrStatusTable from corfuSystem-Namespace.
 */
public abstract class LRMultiNamespaceListener implements StreamListener {

    private boolean snapshotSyncInProgress = false;

    private static final String REPLICATION_STATUS_TABLE = ObjectsView.REPLICATION_STATUS_TABLE;

    @Setter
    long subscriptionTs = 0;

    public void onNextEntry(CorfuStreamEntries results) {
        onNext(results);
    }

    /**
     * A corfu update can/may have multiple updates belonging to different streams.
     * This callback will return those updates as a list grouped by their Stream UUIDs.
     *
     * Note: there is no order guarantee within the transaction boundaries.
     *
     * @param results is a map of stream UUID -> list of entries of this stream.
     */
    public void onNext(CorfuStreamEntries results) {

        Map<TableSchema, List<CorfuStreamEntry>> entries = results.getEntries();

        for (TableSchema tableSchema : entries.keySet()) {
            if (Objects.equals(tableSchema.getTableName(), REPLICATION_STATUS_TABLE)) {
                for (CorfuStreamEntry entry: entries.get(tableSchema)) {
                    ReplicationStatusVal status = (ReplicationStatusVal)entry.getPayload();
                    if (status.getDataConsistent() == false) {
                        snapshotSyncInProgress = true;
                        onSnapshotSyncStart();
                    } else {
                        snapshotSyncInProgress = false;
                        // TODO pankti: This must be invoked only if snapshotSync flag transitions true -> false
                        onSnapshotSyncComplete();
                    }
                }
            } else {
                if (snapshotSyncInProgress) {
                    processUpdateInSnapshotSync(entries);
                } else {
                    processUpdateInLogEntrySync(entries);
                }
            }
        }
    }

    /**
     * -------- Methods to be implemented on the client/application  ---------------
     */
    protected abstract void onSnapshotSyncStart();

    protected abstract void onSnapshotSyncComplete();

    protected abstract void processUpdateInSnapshotSync(Map<TableSchema, List<CorfuStreamEntry>> entries);

    protected abstract void processUpdateInLogEntrySync(Map<TableSchema, List<CorfuStreamEntry>> entries);


    /**
     * Callback to indicate that an error or exception has occurred while streaming or that the stream is
     * shutting down. Some exceptions can be handled by restarting the stream (TrimmedException) while
     * some errors (SystemUnavailableError) are unrecoverable.
     * To be implemented on the client/application
     * @param throwable
     */
    public abstract void onError(Throwable throwable);
}
