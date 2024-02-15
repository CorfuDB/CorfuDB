package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationVersionAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusKey;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.utils.SnapshotSyncUtils;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.NAMESPACE;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.tryOpenTable;
import static org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager.getDefaultSubscriber;
import static org.corfudb.infrastructure.logreplication.utils.LogReplicationUpgradeManager.LOG_REPLICATION_PLUGIN_VERSION_TABLE;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Rolling upgrade handling means cluster must function in a mode where not all
 * nodes are running the same codebase.
 * Here the newer code base must function in a backward compatible mode until all
 * the nodes have been upgraded to the same version.
 * Once all nodes have upgraded it may atomically migrate data, clean up old code and switch
 * completely to newer logic.
 *
 * To facilitate this in LR we add this module which would run once on startup as shown in the
 * following diagram.
 * In addition to running on startup, if cluster is in a mixed mode, all the callers who are
 * mutating data in a new format need to check if upgrade is on and mutate data in a backward
 * compatible manner.
 *
 * For anything post corfu-0.4.0.1 we consider the code base to be at "V2"
 * Anything prior to and including corfu-0.4.0.1 is V1
 * Any code shipped that uses Source/Sink can be thought of as "V2"
 *                  ┌─────────────────────────────────────────────┐
 *                  │ Start transaction to modify new format data │
 *                  └──────────────────┬──────────────────────────┘
 *                                     │
 *                     ┌───────────────▼──────────┐
 *                   ┌─┤  isRolling UpgradeON?    ├─┐
 *         Yes ┌─────┴─┴┐  (migrate if done)     ┌┴─┴─────────────┐ No
 *             │        └────────────────────────┘                │
 *             │                                                  │
 * ┌───────────▼────────────────────┐                     ┌───────▼───────────────────────┐
 * │ Write in both old & new format │                     │ write data in new format only │
 * └───────────┬────────────────────┘                     └───────┬───────────────────────┘
 *             │            ┌──────────────────────┐              │
 *             └───────────►│ end transaction      │◄─────────────┘
 *                          └──────────────────────┘
 */
@Slf4j
public class LRRollingUpgradeHandler {
    public static final String V1_METADATA_TABLE_PREFIX = "CORFU-REPLICATION-WRITER-";
    private volatile boolean isClusterAllAtV2 = false;
    ILogReplicationVersionAdapter versionAdapter;
    CorfuStore corfuStore;

    public LRRollingUpgradeHandler(ILogReplicationVersionAdapter versionAdapter, CorfuStore corfuStore) {
        this.versionAdapter = versionAdapter;
        this.corfuStore = corfuStore;

        // Handle legacy types first.
        LogReplicationMetadataManager.addLegacyTypesToSerializer(corfuStore);
        // Open the status table.
        tryOpenTable(corfuStore, NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME,
                LogReplication.LogReplicationSession.class,
                LogReplication.ReplicationStatus.class, null);
        // Open the event table, which is used to log the intent for triggering a forced snapshot sync upon upgrade
        // completion.
        tryOpenTable(corfuStore, NAMESPACE,
                REPLICATION_EVENT_TABLE_NAME,
                LogReplicationMetadata.ReplicationEventInfoKey.class,
                LogReplicationMetadata.ReplicationEvent.class, null);
    }

    public boolean isLRUpgradeInProgress(CorfuStore corfuStore, TxnContext txnContext) {

        if (isClusterAllAtV2) {
            return false;
        }

        try {
            // If LOG_REPLICATION_PLUGIN_VERSION_TABLE exists, it indicates an upgrade from a
            // previous version was performed. It has since been removed in the current version.
            txnContext.getTable(LOG_REPLICATION_PLUGIN_VERSION_TABLE);
        } catch (NoSuchElementException e) {
            log.info("Version table is not present, setup is a new installation");
            isClusterAllAtV2 = true;
            return false;
        } catch (IllegalArgumentException e) {
            // The table was found but never opened by this runtime.  This means LR has been upgraded from an older
            // version.
            log.info("Version table found but not opened.  This is an old setup being upgraded to LRv2.  Continue.");
        }

        String nodeVersion = versionAdapter.getNodeVersion();

        /**
         * The ideal way to check the versions is to encapsulate the code version
         * into Corfu's Layout information so that even when nodes are down
         * or unresponsive it would be possible to determine if rolling upgrade
         * is running. But since that is a bigger change we resort to the
         * boolean check while ensuring migrateData() is idempotent and is a NO-OP
         * when invoked on a fully upgraded cluster.
         */
        String pinnedClusterVersion = versionAdapter.getPinnedClusterVersion(txnContext);
        boolean isClusterUpgradeInProgress = !nodeVersion.equals(pinnedClusterVersion);
        if (isClusterUpgradeInProgress) {
            return true;
        } // else implies cluster upgrade has completed

        log.info("LRRollingUpgrade upgrade completed to version {}", nodeVersion);
        migrateData(corfuStore, txnContext);
        isClusterAllAtV2 = true;
        return false;
    }

    /**
     * This is the primary function where data is migrated by
     * 1. reading the old format
     * 2. re-writing the data in new format
     * 3. deleting the data in the old format or dropping the old tables
     *
     * @param txnContext All of the above must execute in the same transaction passed in.
     */
    public void migrateData(CorfuStore corfuStore, TxnContext txnContext) {
        // Build sessions from old metadata before it is cleared
        List<LogReplicationSession> sessions = buildSessionsFromOldMetadata(corfuStore, txnContext);

        // Currently only the LogReplicationMetadataManager needs data-migration
        LogReplicationMetadataManager.migrateData(txnContext);
        addSnapshotSyncEventOnUpgradeCompletion(txnContext, sessions);
    }

    /**
     * This is a helper method that is used to construct the sessions from the existing V1 metadata
     * instead of otherwise relying on the session manager. Intended for FULL_TABLE replication.
     * <ul>
     * The way this works can be broken down into three steps:
     * <li>Obtain the sink cluster IDs from the keys of the status table.</li>
     * <li>Get all cluster IDs from the metadata table and filter out the sink IDs from step 1 which
     *  will leave us with just the IDs of the source clusters.</li>
     * <li>Construct sessions from the cartesian product of source/sink cluster IDs. For example,
     * the product of {A, B} * {1, 2} is as follows {(A, 1), (A, 2), (B, 1), (B, 2)}</li>
     * </ul>
     */
    @VisibleForTesting
    public List<LogReplicationSession> buildSessionsFromOldMetadata(CorfuStore corfuStore, TxnContext txnContext) {
        List<LogReplicationSession> sessions = new ArrayList<>();

        // Get the sinkClusterIds from the keys of the status table entries, since the sink clusters
        // write with their localCLusterId and the source clusters write with a remoteClusterId which
        // will map to a sink, we can assume that the statusTableKeys will have just the sink IDs.
        List<ReplicationStatusKey> statusTableKeys = new ArrayList<>(txnContext.keySet(REPLICATION_STATUS_TABLE_NAME));
        List<String> sinkClusterIds = statusTableKeys.stream()
                .map(ReplicationStatusKey::getClusterId)
                .collect(Collectors.toList());

        if (sinkClusterIds.isEmpty()) {
            log.info("No V1 metadata found");
        } else {
            // Get the localClusterIds from suffix's of the old metadata table names and filter out the
            // known sink clusters IDs from the status table to get the sourceClusterIds.
            List<String> sourceClusterIds = corfuStore.listTables(CORFU_SYSTEM_NAMESPACE)
                    .stream()
                    .map(TableName::getTableName)
                    .filter(fullName -> fullName.startsWith(V1_METADATA_TABLE_PREFIX))
                    .map(fullName -> fullName.substring(V1_METADATA_TABLE_PREFIX.length()))
                    .filter(remoteId -> !sinkClusterIds.contains(remoteId))
                    .collect(Collectors.toList());

            // Construct the sessions using the source and sink cluster IDs.
            for (String sourceClusterId : sourceClusterIds) {
                sessions.addAll(sinkClusterIds.stream()
                        .map(remoteId -> LogReplicationSession.newBuilder()
                                .setSourceClusterId(sourceClusterId)
                                .setSinkClusterId(remoteId)
                                .setSubscriber(getDefaultSubscriber())
                                .build())
                        .collect(Collectors.toList()));
            }
        }

        return sessions;
    }

    /**
     * Add flag to event table to trigger snapshot sync.
     */
    private void addSnapshotSyncEventOnUpgradeCompletion(TxnContext txnContext, List<LogReplicationSession> sessions) {
        for (LogReplicationSession session : sessions) {
            SnapshotSyncUtils.addUpgradeSnapshotSyncEvent(session, txnContext);
        }
    }
}
