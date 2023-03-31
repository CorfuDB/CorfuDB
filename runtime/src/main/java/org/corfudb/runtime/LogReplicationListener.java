package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Address;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;

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

    // Indicates if a full sync on client tables was performed during subscription.  A full sync will not be
    // performed if a snapshot sync is ongoing.
    @Getter
    private final AtomicBoolean clientFullSyncPending = new AtomicBoolean(false);

    // This variable tracks if a snapshot sync is ongoing
    @Getter
    private final AtomicBoolean snapshotSyncInProgress = new AtomicBoolean(false);

    // Timestamp at which the client performed a full sync.  Any updates below this timestamp must be ignored.
    // At the time of subscription, a full sync cannot be performed if LR Snapshot Sync is in progress.  Full Sync is
    // performed when this ongoing snapshot sync completes.  The listener, however, can get updates before this full
    // sync.  So we need to maintain this timestamp and ignore any updates below it.
    @Getter
    private final AtomicLong clientFullSyncTimestamp = new AtomicLong(Address.NON_ADDRESS);

    private final CorfuStore corfuStore;
    private final String namespace;

    /**
     * Special LogReplication listener which a client creates to receive ordered updates for replicated data.
     * @param corfuStore Corfu Store used on the client
     * @param namespace Namespace of the client's tables
     */
    public LogReplicationListener(CorfuStore corfuStore, @Nonnull String namespace) {
        this.corfuStore = corfuStore;
        this.namespace = namespace;
    }

    /**
     * This is an internal method of this abstract listener and not exposed to clients.
     *
     * @param results is a map of stream UUID -> list of entries of this stream.
     */
    public final void onNext(CorfuStreamEntries results) {

        // If this update came before the client's full sync timestamp, ignore it.
        if (results.getTimestamp().getSequence() <= clientFullSyncTimestamp.get()) {
            return;
        }

        Set<String> tableNames =
                results.getEntries().keySet().stream().map(schema -> schema.getTableName()).collect(Collectors.toSet());

        if (tableNames.contains(REPLICATION_STATUS_TABLE_NAME)) {
            Preconditions.checkState(results.getEntries().keySet().size() == 1,
                "Replication Status Table Update received with other tables");
            processReplicationStatusUpdate(results);
            return;
        }

        // Data Updates
        if (clientFullSyncPending.get()) {
            // If the listener started when snapshot sync was ongoing, ignore all data updates until it ends.  When
            // it ends, the client will perform a full sync and build a consistent state containing these updates.
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

        List<CorfuStreamEntry> replicationStatusTableEntries =
            entries.entrySet().stream().filter(e -> e.getKey().getTableName().equals(REPLICATION_STATUS_TABLE_NAME))
            .map(Map.Entry::getValue)
            .findFirst()
            .get();

        for (CorfuStreamEntry entry : replicationStatusTableEntries) {

            LogReplicationSession session = (LogReplicationSession)entry.getKey();

            // Only process updates where operation type == UPDATE, model == Logical Groups and the client name
            // matches
            if (entry.getOperation() == CorfuStreamEntry.OperationType.UPDATE &&
                session.getSubscriber().getModel().equals(ReplicationModel.LOGICAL_GROUPS) &&
                Objects.equals(session.getSubscriber().getClientName(), getClientName())) {
                ReplicationStatus status = (ReplicationStatus)entry.getPayload();

                if (status.getSinkStatus().getDataConsistent()) {
                    // getDataConsistent() == true means that snapshot sync has ended.
                    if (snapshotSyncInProgress.get()) {
                        if (clientFullSyncPending.get()) {
                            // Snapshot sync which was ongoing when the listener was subscribed has ended.  Attempt to
                            // perform a full sync now.
                            LogReplicationUtils.attemptClientFullSync(corfuStore, this, namespace);
                            return;
                        }
                        // Process snapshot sync completion in steady state, i.e., client full sync is already complete
                        snapshotSyncInProgress.set(false);
                        onSnapshotSyncComplete();
                    }
                } else {
                    // getDataConsistent() == false.  Snapshot sync has started.
                    snapshotSyncInProgress.set(true);
                    onSnapshotSyncStart();
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
     * read all application tables which the client is interested in merging together and perform the merge.
     * @param txnContext transaction context in which the operation must be performed
     */
    protected abstract void performFullSyncAndMerge(TxnContext txnContext);

    /**
     * Name with which this client was registered using the interfaces in LogReplicationLogicalGroupClient
     * @return client name
     */
    protected abstract String getClientName();

    /**
     * Callback to indicate that an error or exception has occurred while streaming or that the stream is
     * shutting down. Some exceptions can be handled by restarting the stream (TrimmedException) while
     * some errors (SystemUnavailableError) are unrecoverable.
     * To be implemented on the client/application
     * @param throwable
     */
    public abstract void onError(Throwable throwable);
}
