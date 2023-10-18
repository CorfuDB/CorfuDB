package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ILogReplicationVersionAdapter;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;

import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.NAMESPACE;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME;
import static org.corfudb.infrastructure.logreplication.utils.LogReplicationUpgradeManager.LOG_REPLICATION_PLUGIN_VERSION_TABLE;

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

    private volatile boolean isClusterAllAtV2 = false;
    ILogReplicationVersionAdapter versionAdapter;

    CorfuStore corfuStore;

    public LRRollingUpgradeHandler(ILogReplicationVersionAdapter versionAdapter, CorfuStore corfuStore) {
        this.versionAdapter = versionAdapter;
        this.corfuStore = corfuStore;

        // Handle legacy types first.
        LogReplicationMetadataManager.addLegacyTypesToSerializer(corfuStore);
        LogReplicationMetadataManager.tryOpenTable(corfuStore, NAMESPACE,
                LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME,
                LogReplication.LogReplicationSession.class,
                LogReplication.ReplicationStatus.class, null);
        // Open the event table, which is used to log the intent for triggering a forced snapshot sync upon upgrade
        // completion
        LogReplicationMetadataManager.tryOpenTable(corfuStore, NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                LogReplication.ReplicationEventInfoKey.class, LogReplication.ReplicationEvent.class, null);
    }

    public boolean isLRUpgradeInProgress(TxnContext txnContext) {

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
        migrateData(txnContext);
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
    public void migrateData(TxnContext txnContext) {
        // Currently only the LogReplicationMetadataManager needs data-migration
        LogReplicationMetadataManager.migrateData(txnContext);
        addSnapshotSyncEventOnUpgradeCompletion(txnContext);
    }

    /**
     * Add flag to event table to trigger snapshot sync.
     */
    public void addSnapshotSyncEventOnUpgradeCompletion(TxnContext txnContext) {
        UUID rollingUpgradeForceSyncId = UUID.randomUUID();

        // Write a rolling upgrade force snapshot sync event to the logReplicationEventTable
        LogReplication.ReplicationEventInfoKey key = LogReplication.ReplicationEventInfoKey.newBuilder()
                .build();

        LogReplication.ReplicationEvent event = LogReplication.ReplicationEvent.newBuilder()
                .setEventId(rollingUpgradeForceSyncId.toString())
                .setType(LogReplication.ReplicationEvent.ReplicationEventType.UPGRADE_COMPLETION_FORCE_SNAPSHOT_SYNC)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .build();

        Table<LogReplication.ReplicationEventInfoKey, LogReplication.ReplicationEvent, Message> replicationEventTable =
                txnContext.getTable(REPLICATION_EVENT_TABLE_NAME);

        log.info("Forced snapshot sync will be triggered due to completion of rolling upgrade");
        txnContext.putRecord(replicationEventTable, key, event, null);
    }
}
