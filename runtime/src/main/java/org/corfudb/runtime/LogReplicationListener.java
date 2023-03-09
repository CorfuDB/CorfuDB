package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.ReplicationStatusVal;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE;

/**
 * This is the interface that a client must subscribe to if it needs to observe and bifurcate the data updates received
 * on Log Entry and Snapshot Sync.  The client's usecase is that it maintains a 'merged table' which contains data
 * received through replication and local updates.  Log Replicator does not write to this merged table.  This
 * listener will observe the writes and apply them to the merged table based on the client implementation.
 *
 *
 * This interface sees ordered updates from :
 * 1. client-streams from client-Namespace, and,
 * 2. LrStatusTable from corfuSystem-Namespace.
 *
 * The client implementing this interface will only observe the data updates from client streams
 */
@Slf4j
public abstract class LogReplicationListener implements StreamListener {

    // Indicates if this listener was subscribed when snapshot sync was in progress
    @Getter
    private final AtomicBoolean snapshotSyncInProgressOnSubscription = new AtomicBoolean(false);

    // This variable tracks the status of snapshot sync only after snapshotSyncInProgressOnSubscription becomes false
    private final AtomicBoolean snapshotSyncInProgress = new AtomicBoolean(false);

    private final CorfuStore corfuStore;
    private final String namespace;
    private final String streamTag;
    private final List<String> tablesOfInterest;
    private final int bufferSize;

    public LogReplicationListener(CorfuStore corfuStore, @Nonnull String namespace, @Nonnull String streamTag,
                                  @Nonnull List<String> tablesOfInterest) {
        this(corfuStore, namespace, streamTag, tablesOfInterest, 0);
    }

    public LogReplicationListener(CorfuStore corfuStore, @Nonnull String namespace, @Nonnull String streamTag,
                                  int bufferSize) {
        this(corfuStore, namespace, streamTag, null, bufferSize);
    }

    public LogReplicationListener(CorfuStore corfuStore, @Nonnull String namespace, @Nonnull String streamTag,
                                  List<String> tablesOfInterest, int bufferSize) {
        this.corfuStore = corfuStore;
        this.namespace = namespace;
        this.streamTag = streamTag;
        this.tablesOfInterest = tablesOfInterest;
        this.bufferSize = bufferSize;
    }

    /**
     * This is an internal method of this abstract listener and not exposed to clients.
     *
     * @param results is a map of stream UUID -> list of entries of this stream.
     */
    public final void onNext(CorfuStreamEntries results) {

        // A corfu update can/may have multiple updates belonging to different streams.  This callback will return
        // those updates as a list grouped by their Stream UUIDs.
        // Note: there is no order guarantee within the transaction boundaries.

        Set<String> tableNames =
                results.getEntries().keySet().stream().map(schema -> schema.getTableName()).collect(Collectors.toSet());

        if (tableNames.contains(REPLICATION_STATUS_TABLE)) {
            Preconditions.checkState(results.getEntries().keySet().size() == 1,
                "Replication Status Table Update received with other tables");
            processReplicationStatusUpdate(results);
            return;
        }

        // Data Updates
        if (snapshotSyncInProgressOnSubscription.get()) {
            // If the listener started when snapshot sync was already ongoing, ignore all data updates until it ends
            return;
        }

        if (snapshotSyncInProgress.get()) {
            processUpdatesInSnapshotSync(results);
        } else {
            processUpdatesInLogEntrySync(results);
        }
    }

    private void processReplicationStatusUpdate(CorfuStreamEntries results) {
        Map<TableSchema, List<CorfuStreamEntry>> entries = results.getEntries();

        TableSchema replicationStatusTableSchema =
                entries.keySet().stream().filter(key -> key.getTableName().equals(REPLICATION_STATUS_TABLE))
                        .findFirst().get();

        for (CorfuStreamEntry entry : entries.get(replicationStatusTableSchema)) {

            // Ignore any update where the operation type != UPDATE
            if (entry.getOperation() == CorfuStreamEntry.OperationType.UPDATE) {
                ReplicationStatusVal status = (ReplicationStatusVal)entry.getPayload();

                if (snapshotSyncInProgressOnSubscription.get() && status.getDataConsistent() == true) {
                    snapshotSyncInProgressOnSubscription.set(false);
                    corfuStore.unsubscribeListener(this);

                    if (bufferSize == 0) {
                        corfuStore.subscribeLogReplicationListener(this, namespace, streamTag,
                                tablesOfInterest);
                    } else if (tablesOfInterest == null) {
                        corfuStore.subscribeLogReplicationListener(this, namespace, streamTag, bufferSize);
                    } else {
                        corfuStore.subscribeLogReplicationListener(this, namespace, streamTag, tablesOfInterest,
                                bufferSize);
                    }
                    return;
                }

                if (!status.getDataConsistent()) {
                    snapshotSyncInProgress.set(true);
                    onSnapshotSyncStart();
                } else if (snapshotSyncInProgress.get()) {
                    snapshotSyncInProgress.set(false);
                    onSnapshotSyncComplete();
                }
            }
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
    protected abstract void processUpdatesInSnapshotSync(CorfuStreamEntries results);

    /**
     * Invoked when data updates are received as part of a LogEntry Sync.
     * @param results Entries received in a single transaction as part of a log entry sync
     */
    protected abstract void processUpdatesInLogEntrySync(CorfuStreamEntries results);

    /**
     * Invoked by the Corfu runtime when this listener is being subscribed.  This method should
     * perform a full-sync on all application tables which the client is interested in merging together.
     * @param txnContext transaction context in which the operation must be performed
     */
    protected abstract void performFullSync(TxnContext txnContext);

    /**
     * Callback to indicate that an error or exception has occurred while streaming or that the stream is
     * shutting down. Some exceptions can be handled by restarting the stream (TrimmedException) while
     * some errors (SystemUnavailableError) are unrecoverable.
     * To be implemented on the client/application
     * @param throwable
     */
    public abstract void onError(Throwable throwable);
}
