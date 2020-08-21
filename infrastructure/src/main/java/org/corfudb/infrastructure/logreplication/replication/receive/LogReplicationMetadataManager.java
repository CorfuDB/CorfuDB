package org.corfudb.infrastructure.logreplication.replication.receive;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataVal;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.QueryResult;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.Address;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * The table persisted at the replication writer side.
 * It records the log reader cluster's snapshot timestamp and last log entry's timestamp, it has received and processed.
 */
@Slf4j
public class LogReplicationMetadataManager {

    private static final String NAMESPACE = CORFU_SYSTEM_NAMESPACE;
    private static final String METADATA_TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";
    private static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";

    private CorfuStore corfuStore;

    private String metadataTableName;

    private Table<LogReplicationMetadataKey, LogReplicationMetadataVal, LogReplicationMetadataVal> metadataTable;
    private Table<ReplicationStatusKey, ReplicationStatusVal, ReplicationStatusVal> replicationStatusTable;

    private CorfuRuntime runtime;
    private String localClusterId;

    public LogReplicationMetadataManager(CorfuRuntime rt, long topologyConfigId, String localClusterId) {
        this.runtime = rt;
        this.corfuStore = new CorfuStore(runtime);
        metadataTableName = getPersistedWriterMetadataTableName(localClusterId);
        try {
            metadataTable = this.corfuStore.openTable(NAMESPACE,
                            metadataTableName,
                            LogReplicationMetadataKey.class,
                            LogReplicationMetadataVal.class,
                            null,
                            TableOptions.builder().build());
            replicationStatusTable = this.corfuStore.openTable(NAMESPACE,
                            REPLICATION_STATUS_TABLE,
                            ReplicationStatusKey.class,
                            ReplicationStatusVal.class,
                            null,
                            TableOptions.builder().build());
            this.localClusterId = localClusterId;
        } catch (Exception e) {
            log.error("Caught an exception while opening MetadataManagerTables {}", e);
            throw new ReplicationWriterException(e);
        }
        setupTopologyConfigId(topologyConfigId);
    }

    public CorfuStoreMetadata.Timestamp getTimestamp() {
        return corfuStore.getTimestamp();
    }

    public TxBuilder getTxBuilder() {
        return corfuStore.tx(NAMESPACE);
    }

    private String queryString(CorfuStoreMetadata.Timestamp timestamp, LogReplicationMetadataType key) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        CorfuRecord record;
        if (timestamp == null) {
            record = corfuStore.query(NAMESPACE).getRecord(metadataTableName, txKey);
        } else {
            record = corfuStore.query(NAMESPACE).getRecord(metadataTableName, timestamp, txKey);
        }

        LogReplicationMetadataVal metadataVal = null;
        String val = null;

        if (record != null) {
            metadataVal = (LogReplicationMetadataVal)record.getPayload();
        }

        if (metadataVal != null) {
            val = metadataVal.getVal();
        }

        return val;
    }

    public long query(CorfuStoreMetadata.Timestamp timestamp, LogReplicationMetadataType key) {
        long val = -1;
        String str = queryString(timestamp, key);
        if (str != null) {
            val = Long.parseLong(str);
        }
        return val;
    }

    public long getTopologyConfigId() {
        return query(null, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
    }

    public String getVersion() {
        return queryString(null, LogReplicationMetadataType.VERSION);
    }

    public long getLastSnapStartTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
    }

    public long getLastSnapTransferDoneTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED);
    }

    public long getLastAppliedBaseSnapshotTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED);
    }

    public long getLastSnapSeqNum() {
        return query(null, LogReplicationMetadataType.LAST_SNAPSHOT_SEQ_NUM);
    }

    public long getLastProcessedLogTimestamp() {
        return query(null, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED);
    }

    public void appendUpdate(TxBuilder txBuilder, LogReplicationMetadataType type, long val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(type.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(Long.toString(val)).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    private void appendUpdate(TxBuilder txBuilder, LogReplicationMetadataType key, String val) {
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();
        LogReplicationMetadataVal txVal = LogReplicationMetadataVal.newBuilder().setVal(val).build();
        txBuilder.update(metadataTableName, txKey, txVal, null);
    }

    public void setupTopologyConfigId(long topologyConfigId) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistedTopologyConfigId = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);

        if (topologyConfigId <= persistedTopologyConfigId) {
            log.warn("Skip setupTopologyConfigId. the current topologyConfigId {} is not larger than the persistedTopologyConfigID {}",
                topologyConfigId, persistedTopologyConfigId);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);

        for (LogReplicationMetadataType type : LogReplicationMetadataType.values()) {
            long val = Address.NON_ADDRESS;
            if (type == LogReplicationMetadataType.TOPOLOGY_CONFIG_ID) {
                val = topologyConfigId;
            }
            appendUpdate(txBuilder, type, val);
         }
        txBuilder.commit(timestamp);
        log.info("Update topologyConfigId, new metadata {}", this);
    }

    public void updateVersion(String version) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        String  persistedVersion = queryString(timestamp, LogReplicationMetadataType.VERSION);

        if (persistedVersion.equals(version)) {
            log.warn("Skip update of the current version {} to {} as they are the same",
                persistedVersion, version);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);

        for (LogReplicationMetadataType key : LogReplicationMetadataType.values()) {
            long val = Address.NON_ADDRESS;

            // For version, it will be updated with the current version
            if (key == LogReplicationMetadataType.VERSION) {
                appendUpdate(txBuilder, key, version);
            } else if (key == LogReplicationMetadataType.TOPOLOGY_CONFIG_ID) {
                // For siteConfig ID, it should not be changed. Update it to fence off other metadata updates.
                val = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
                appendUpdate(txBuilder, key, val);
            } else {
                // Reset all other keys to -1.
                appendUpdate(txBuilder, key, val);
            }
        }

        txBuilder.commit(timestamp);
    }

    /**
     * If the current topologyConfigId is not the same as the persisted topologyConfigId, ignore the operation.
     * If the current ts is smaller than the persisted snapStart, it is an old operation,
     * ignore it it.
     * Otherwise, update the snapStart. The update of topologyConfigId just fence off any other metadata
     * updates in another transactions.
     *
     * @param topologyConfigId the current operation's topologyConfigId
     * @param ts the snapshotStart snapshot time for the topologyConfigId.
     * @return if the operation succeeds or not.
     */
    public boolean setSrcBaseSnapshotStart(long topologyConfigId, long ts) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistedTopologyConfigID = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistSnapStart = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);

        log.debug("Set snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}, persistedSnapshotStart={}",
                topologyConfigId, ts, persistedTopologyConfigID, persistSnapStart);

        // It means the cluster config has changed, ignore the update operation.
        if (topologyConfigId != persistedTopologyConfigID || ts <= persistedTopologyConfigID) {
            log.warn("The metadata is older than the persisted one. Set snapshotStart topologyConfigId={}, ts={}," +
                    " persistedTopologyConfigId={}, persistedSnapshotStart={}", topologyConfigId, ts,
                    persistedTopologyConfigID, persistSnapStart);
            return false;
        }

        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);

        // Update the topologyConfigId to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);

        // Setup the LAST_LAST_SNAPSHOT_STARTED
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, ts);

        // Reset other metadata
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_SEQ_NUM, Address.NON_ADDRESS);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED, Address.NON_ADDRESS);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}, " +
                        "persistedSnapshotStart={}",
                topologyConfigId, ts, persistedTopologyConfigID, persistSnapStart);

        return (ts == getLastSnapStartTimestamp() && topologyConfigId == getTopologyConfigId());
    }


    /**
     * This call should be done in a transaction after a transfer done and before apply the snapshot.
     * @param ts
     */
    public void setLastSnapTransferDoneTimestamp(long topologyConfigId, long ts) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistedTopologyConfigId = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistSnapStart = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);

        log.debug("setLastSnapTransferDone snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}," +
                " persistedSiteConfigID={}, persistedSnapshotStart={}", topologyConfigId, ts, persistedTopologyConfigId,
                persistSnapStart);

        // It means the cluster config has changed, ignore the update operation.
        if (topologyConfigId != persistedTopologyConfigId || ts <= persistedTopologyConfigId) {
            log.warn("The metadata is older than the persisted one. Set snapshotStart topologyConfigId " + topologyConfigId + " ts " + ts +
                    " persisteSiteConfigID " + persistedTopologyConfigId + " persistSnapStart " + persistSnapStart);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);

        //Update the topologyConfigId to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);

        //Setup the LAST_LAST_SNAPSHOT_STARTED
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED, ts);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart topologyConfigId " + topologyConfigId + " ts " + ts +
                " persisteSiteConfigID " + persistedTopologyConfigId + " persistSnapStart " + persistSnapStart);
    }

    public void setSnapshotApplied(LogReplicationEntry entry) {
        CorfuStoreMetadata.Timestamp timestamp = corfuStore.getTimestamp();
        long persistSiteConfigID = query(timestamp, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
        long persistSnapStart = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
        long persistSnapTranferDone = query(timestamp, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED);
        long siteConfigID = entry.getMetadata().getTopologyConfigId();
        long ts = entry.getMetadata().getSnapshotTimestamp();

        if (siteConfigID != persistSiteConfigID || ts != persistSnapStart || ts != persistSnapTranferDone) {
            log.warn("topologyConfigId " + siteConfigID + " != " + " persist " + persistSiteConfigID +  " ts " + ts +
                    " != " + "persistSnapTransferDone " + persistSnapTranferDone);
            return;
        }

        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);

        //Update the topologyConfigId to fence all other transactions that update the metadata at the same time
        appendUpdate(txBuilder, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, siteConfigID);

        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, ts);
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED, ts);

        //may not need
        appendUpdate(txBuilder, LogReplicationMetadataType.LAST_SNAPSHOT_SEQ_NUM, Address.NON_ADDRESS);

        txBuilder.commit(timestamp);

        log.debug("Commit. Set snapshotStart topologyConfigId " + siteConfigID + " ts " + ts +
                " persistSiteConfigID " + persistSiteConfigID + " persistSnapStart " + persistSnapStart);
    }

    public void setReplicationRemainingEntries(String clusterId, long remainingEntries,
                                               ReplicationStatusVal.SyncType type) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();
        ReplicationStatusVal val = ReplicationStatusVal.newBuilder().setRemainingEntriesToSend(remainingEntries)
                .setType(type).build();
        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);
        txBuilder.update(REPLICATION_STATUS_TABLE, key, val, null);
        txBuilder.commit();

        log.trace("setReplicationRemainingEntries: clusterId: {}, remainingEntries: {}, type: {}",
                clusterId, remainingEntries, type);
    }

    public Map<String, ReplicationStatusVal> getReplicationRemainingEntries() {
        Map<String, ReplicationStatusVal> replicationStatusMap = new HashMap<>();
        QueryResult<CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, ReplicationStatusVal>> entries =
                corfuStore.query(NAMESPACE).executeQuery(REPLICATION_STATUS_TABLE, record -> true);

        for(CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, ReplicationStatusVal>entry : entries.getResult()) {
            replicationStatusMap.put(entry.getKey().getClusterId(), entry.getPayload());
        }

        log.debug("getReplicationRemainingEntries: replicationStatusMap size: {}", replicationStatusMap.size());

        return replicationStatusMap;
    }

    public ReplicationStatusVal getReplicationRemainingEntries(String clusterId) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();
        CorfuRecord record = corfuStore.query(NAMESPACE).getRecord(REPLICATION_STATUS_TABLE, key);
        if (record == null) {
            return null;
        }
        return (ReplicationStatusVal)record.getPayload();
    }

    public void setDataConsistentOnStandby(boolean isConsistent) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(localClusterId).build();
        ReplicationStatusVal val = ReplicationStatusVal.newBuilder().setDataConsistent(isConsistent).build();
        TxBuilder txBuilder = corfuStore.tx(NAMESPACE);
        txBuilder.update(REPLICATION_STATUS_TABLE, key, val, null);
        txBuilder.commit();

        log.trace("setDataConsistentOnStandby: localClusterId: {}, isConsistent: {}", localClusterId, isConsistent);
    }

    public Map<String, ReplicationStatusVal> getDataConsistentOnStandby() {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(localClusterId).build();
        CorfuRecord record = corfuStore.query(NAMESPACE).getRecord(REPLICATION_STATUS_TABLE, key);

        ReplicationStatusVal statusVal;
        // Initially, snapshot sync is pending so the data is not consistent.
        if (record == null) {
            log.warn("No Key for Data Consistent found.  DataConsistent Status is not set.");
            statusVal = ReplicationStatusVal.newBuilder().setDataConsistent(false).build();
        } else {
            statusVal = (ReplicationStatusVal)record.getPayload();
        }
        Map<String, ReplicationStatusVal> dataConsistentMap = new HashMap<>();
        dataConsistentMap.put(localClusterId, statusVal);

        log.debug("getDataConsistentOnStandby: localClusterId: {}, statusVal: {}", localClusterId, statusVal);

        return dataConsistentMap;
    }

    public void resetReplicationStatus() {
        replicationStatusTable.clear();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (LogReplicationMetadataType type : LogReplicationMetadataType.values()) {
            builder.append(type).append(": ");
            switch (type) {
                case TOPOLOGY_CONFIG_ID:
                    builder.append(getTopologyConfigId());
                    break;
                case LAST_SNAPSHOT_STARTED:
                   builder.append(getLastSnapStartTimestamp());
                   break;
                case LAST_SNAPSHOT_TRANSFERRED:
                   builder.append(getLastSnapTransferDoneTimestamp());
                   break;
                case LAST_SNAPSHOT_APPLIED:
                   builder.append(getLastAppliedBaseSnapshotTimestamp());
                   break;
                case LAST_SNAPSHOT_SEQ_NUM:
                   builder.append(getLastSnapSeqNum());
                   break;
                case LAST_LOG_ENTRY_PROCESSED:
                   builder.append(getLastProcessedLogTimestamp());
                   break;
                default:
                    // error
            }
            builder.append(" ");
        }
        builder.append("Replication Completion: ");
        Map<String, ReplicationStatusVal> replicationStatusMap = getReplicationRemainingEntries();
        replicationStatusMap.entrySet().forEach( entry -> builder.append(entry.getKey())
                .append(entry.getValue().getRemainingEntriesToSend()));

        builder.append("Data Consistent: ").append(getDataConsistentOnStandby());
        return builder.toString();
    }

    public static String getPersistedWriterMetadataTableName(String localClusterId) {
        return METADATA_TABLE_PREFIX_NAME + localClusterId;
    }

    public long getLogHead() {
        return runtime.getAddressSpaceView().getTrimMark().getSequence();
    }

    /**
     * Set the snapshot sync start marker, i.e., a unique identification of the current snapshot sync cycle.
     * Identified by the snapshot sync Id and the min shadow stream update timestamp for this cycle.
     *
     * @param currentSnapshotSyncId
     * @param shadowStreamTs
     */
    public void setSnapshotSyncStartMarker(UUID currentSnapshotSyncId, CorfuStoreMetadata.Timestamp shadowStreamTs, TxBuilder txBuilder) {

        long currentSnapshotSyncIdLong = currentSnapshotSyncId.getMostSignificantBits() & Long.MAX_VALUE;
        long persistedSnapshotId = query(null, LogReplicationMetadataType.CURRENT_SNAPSHOT_CYCLE_ID);

        if (persistedSnapshotId != currentSnapshotSyncIdLong) {
            // Update if current Snapshot Sync differs from the persisted one, otherwise ignore.
            // It could have already been updated in the case that leader changed in between a snapshot sync cycle
            appendUpdate(txBuilder, LogReplicationMetadataType.CURRENT_SNAPSHOT_CYCLE_ID, currentSnapshotSyncIdLong);
            appendUpdate(txBuilder, LogReplicationMetadataType.CURRENT_CYCLE_MIN_SHADOW_STREAM_TS, shadowStreamTs.getSequence());
        }
    }

    /**
     * Retrieve the snapshot sync start marker
     **/
    public long getMinSnapshotSyncShadowStreamTs() {
        return query(null, LogReplicationMetadataType.CURRENT_CYCLE_MIN_SHADOW_STREAM_TS);
    }

    /**
     * Retrieve the current snapshot sync cycle Id
     */
    public long getCurrentSnapshotSyncCycleId() {
        return query(null, LogReplicationMetadataType.CURRENT_SNAPSHOT_CYCLE_ID);
    }

    public enum LogReplicationMetadataType {
        TOPOLOGY_CONFIG_ID("topologyConfigId"),
        VERSION("version"),
        LAST_SNAPSHOT_STARTED("lastSnapStart"),
        LAST_SNAPSHOT_TRANSFERRED("lastSnapTransferred"),
        LAST_SNAPSHOT_APPLIED("lastSnapApplied"),
        LAST_SNAPSHOT_SEQ_NUM("lastSnapSeqNum"),
        CURRENT_SNAPSHOT_CYCLE_ID("currentSnapshotCycleId"),
        CURRENT_CYCLE_MIN_SHADOW_STREAM_TS("minShadowStreamTimestamp"),
        LAST_LOG_ENTRY_PROCESSED("lastLogProcessed"),
        REMAINING_REPLICATION_PERCENT("replicationStatus"),
        DATA_CONSISTENT_ON_STANDBY("dataConsistentOnStandby");

        @Getter
        String val;
        LogReplicationMetadataType(String newVal) {
            val  = newVal;
        }
    }
}
