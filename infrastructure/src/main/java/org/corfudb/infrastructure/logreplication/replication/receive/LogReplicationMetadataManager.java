package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationMetadataVal;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SnapshotSyncInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SyncStatus;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.Address;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * The table persisted at the replication writer side.
 * It records the log reader cluster's snapshot timestamp and last log entry's timestamp, it has received and processed.
 */
@Slf4j
public class LogReplicationMetadataManager {

    public static final String NAMESPACE = CORFU_SYSTEM_NAMESPACE;
    public static final String METADATA_TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";
    private static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";
    private static final String REPLICATION_EVENT_TABLE_NAME = "LogReplicationEventTable";

    private final CorfuStore corfuStore;

    private final String metadataTableName;

    private final CorfuRuntime runtime;
    private final String localClusterId;

    private final Table<ReplicationStatusKey, ReplicationStatusVal, Message> replicationStatusTable;
    private final Table<LogReplicationMetadataKey, LogReplicationMetadataVal, Message> metadataTable;
    private final Table<ReplicationEventKey, ReplicationEvent, Message> replicationEventTable;

    public LogReplicationMetadataManager(CorfuRuntime rt, long topologyConfigId, String localClusterId) {

        // LR does not require transaction logging enabled as we don't want to trigger subscriber's logic
        // on replicated data which could eventually lead to overwrites
        this.runtime = rt;
        // CorfuStore must have TX Logging Enabled as it might be used by consumers to update UI
        this.corfuStore = new CorfuStore(runtime);

        metadataTableName = getPersistedWriterMetadataTableName(localClusterId);
        try {
            this.metadataTable = this.corfuStore.openTable(NAMESPACE,
                            metadataTableName,
                            LogReplicationMetadataKey.class,
                            LogReplicationMetadataVal.class,
                            null,
                            TableOptions.builder().build());

            this.replicationStatusTable = this.corfuStore.openTable(NAMESPACE,
                            REPLICATION_STATUS_TABLE,
                            ReplicationStatusKey.class,
                            ReplicationStatusVal.class,
                            null,
                            TableOptions.builder().build());

            this.replicationEventTable = this.corfuStore.openTable(NAMESPACE,
                    REPLICATION_EVENT_TABLE_NAME,
                    ReplicationEventKey.class,
                    ReplicationEvent.class,
                    null,
                    TableOptions.builder().build());

            this.localClusterId = localClusterId;
        } catch (Exception e) {
            log.error("Caught an exception while opening MetadataManagerTables ", e);
            throw new ReplicationWriterException(e);
        }
        setupTopologyConfigId(topologyConfigId);
    }

    public TxnContext getTxnContext() {
        return corfuStore.txn(NAMESPACE);
    }

    private String queryString(LogReplicationMetadataType key) {
        CorfuStoreEntry record;
        LogReplicationMetadataKey txKey = LogReplicationMetadataKey.newBuilder().setKey(key.getVal()).build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            record = txn.getRecord(metadataTableName, txKey);
            txn.commit();
        }

        if (record.getPayload() != null) {
            LogReplicationMetadataVal metadataVal = (LogReplicationMetadataVal)record.getPayload();

            if (metadataVal != null) {
                return metadataVal.getVal();
            }
        }

        return null;
    }

    /**
     * Query multiple Log Replication Metadata keys across the same timestamp
     * TODO: this table should be reformatted such that metadata is accessed with a single RPC call (group keys)
     *    this should be done later as it will require a data migration task
     *
     * @param keyTypes all metadata key types to query across the same timestamp
     * @return
     */
    public Map<LogReplicationMetadataType, Long> queryMetadata(TxnContext txn, LogReplicationMetadataType... keyTypes) {
        Map<LogReplicationMetadataType, Long> metadataMap = new HashMap<>();

        CorfuStoreEntry record;
        String stringValue;
        for (LogReplicationMetadataType keyType : keyTypes) {
            stringValue = null;
                record = txn.getRecord(metadataTableName, LogReplicationMetadataKey.newBuilder().setKey(keyType.getVal()).build());

            if (record.getPayload() != null) {
                LogReplicationMetadataVal metadataValue = (LogReplicationMetadataVal) record.getPayload();

                if (metadataValue != null) {
                    stringValue = metadataValue.getVal();
                }
            }

            metadataMap.put(keyType, stringValue != null ? Long.parseLong(stringValue) : -1L);
        }

        return metadataMap;
    }

    public long queryMetadata(LogReplicationMetadataType key) {
        long val = -1;
        String str = queryString(key);
        if (str != null) {
            val = Long.parseLong(str);
        }
        return val;
    }

    public long getTopologyConfigId() {
        return queryMetadata(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
    }

    public String getVersion() {
        return queryString(LogReplicationMetadataType.VERSION);
    }

    public long getLastStartedSnapshotTimestamp() {
        return queryMetadata(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
    }

    public long getLastTransferredSnapshotTimestamp() {
        return queryMetadata(LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED);
    }

    public long getLastAppliedSnapshotTimestamp() {
        return queryMetadata(LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED);
    }

    public long getLastSnapshotTransferredSequenceNumber() {
        return queryMetadata(LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER);
    }

    public long getLastProcessedLogEntryTimestamp() {
        return queryMetadata(LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED);
    }

    public ResponseMsg getMetadataResponse(HeaderMsg header) {
        LogReplication.LogReplicationMetadataResponseMsg metadataMsg = LogReplication.LogReplicationMetadataResponseMsg
                .newBuilder()
                .setTopologyConfigID(getTopologyConfigId())
                .setVersion(getVersion())
                .setSnapshotStart(getLastStartedSnapshotTimestamp())
                .setSnapshotTransferred(getLastTransferredSnapshotTimestamp())
                .setSnapshotApplied(getLastAppliedSnapshotTimestamp())
                .setLastLogEntryTimestamp(getLastProcessedLogEntryTimestamp()).build();
        CorfuMessage.ResponsePayloadMsg payload = CorfuMessage.ResponsePayloadMsg.newBuilder()
                .setLrMetadataResponse(metadataMsg).build();
        return getResponseMsg(header, payload);
    }

    public void appendUpdate(TxnContext txn, LogReplicationMetadataType keyType, long val) {
        appendUpdate(txn, keyType, LogReplicationMetadataVal.newBuilder().setVal(Long.toString(val)).build());
    }

    private void appendUpdate(TxnContext txn, LogReplicationMetadataType keyType, String val) {
        appendUpdate(txn, keyType, LogReplicationMetadataVal.newBuilder().setVal(val).build());
    }

    private void appendUpdate(TxnContext txn, LogReplicationMetadataType keyType, LogReplicationMetadataVal value) {
        LogReplicationMetadataKey key = LogReplicationMetadataKey.newBuilder().setKey(keyType.getVal()).build();
        txn.putRecord(metadataTable, key, value, null);
    }

    public void setupTopologyConfigId(long topologyConfigId) {
        long persistedTopologyConfigId = queryMetadata(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);

        if (topologyConfigId <= persistedTopologyConfigId) {
            log.warn("Skip setupTopologyConfigId. the current topologyConfigId {} is not larger than the persistedTopologyConfigID {}",
                topologyConfigId, persistedTopologyConfigId);
            return;
        }

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            for (LogReplicationMetadataType type : LogReplicationMetadataType.values()) {
                long val = Address.NON_ADDRESS;
                if (type == LogReplicationMetadataType.TOPOLOGY_CONFIG_ID) {
                    val = topologyConfigId;
                }
                appendUpdate(txn, type, val);
            }
            txn.commit();
        }

        log.info("Update topologyConfigId, new metadata {}", this);
    }

    public void updateVersion(String version) {
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            String persistedVersion = queryMetadata(txn, LogReplicationMetadataType.VERSION)
                    .get(LogReplicationMetadataType.VERSION).toString();

            if (persistedVersion.equals(version)) {
                log.warn("Skip update of the current version {} to {} as they are the same",
                        persistedVersion, version);
                return;
            }

            for (LogReplicationMetadataType key : LogReplicationMetadataType.values()) {
                long val = Address.NON_ADDRESS;

                // For version, it will be updated with the current version
                if (key == LogReplicationMetadataType.VERSION) {
                    appendUpdate(txn, key, version);
                } else if (key == LogReplicationMetadataType.TOPOLOGY_CONFIG_ID) {
                    // For siteConfig ID, it should not be changed. Update it to fence off other metadata updates.
                    val = queryMetadata(txn, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID).get(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
                    appendUpdate(txn, key, val);
                } else {
                    // Reset all other keys to -1.
                    appendUpdate(txn, key, val);
                }
            }
            txn.commit();
        }
    }

    /**
     * Set the snapshot sync base timestamp, i.e., the timestamp of the consistent cut for which
     * data is being replicated.
     *
     * If the current topologyConfigId is not the same as the persisted topologyConfigId, ignore the operation.
     * If the current ts is smaller than the persisted snapStart, it is an old operation,
     * ignore it.
     * Otherwise, update the base snapshot start timestamp. The update of topologyConfigId just fences off
     * any other metadata updates in other transactions.
     *
     * @param topologyConfigId current topologyConfigId
     * @param ts snapshot start timestamp
     * @return true, if succeeds
     *         false, otherwise
     */
    public boolean setBaseSnapshotStart(long topologyConfigId, long ts) {
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            Map<LogReplicationMetadataType, Long> metadataMap = queryMetadata(txn, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID,
                    LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
            long persistedTopologyConfigID = metadataMap.get(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
            long persistedSnapshotStart = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);

            log.debug("Set snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}, persistedSnapshotStart={}",
                    topologyConfigId, ts, persistedTopologyConfigID, persistedSnapshotStart);

            // It means the cluster config has changed, ignore the update operation.
            if (topologyConfigId != persistedTopologyConfigID) {
                log.warn("Config differs between sender and receiver, sender[topologyConfigId={}, ts={}]" +
                                " receiver[persistedTopologyConfigId={}, persistedSnapshotStart={}]", topologyConfigId, ts,
                        persistedTopologyConfigID, persistedSnapshotStart);
                return false;
            }

            // Update the topologyConfigId to fence all other transactions that update the metadata at the same time
            appendUpdate(txn, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);

            // Setup the LAST_LAST_SNAPSHOT_STARTED
            appendUpdate(txn, LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, ts);

            // Reset other metadata
            appendUpdate(txn, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED, Address.NON_ADDRESS);
            appendUpdate(txn, LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, Address.NON_ADDRESS);
            appendUpdate(txn, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER, Address.NON_ADDRESS);
            appendUpdate(txn, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED, Address.NON_ADDRESS);

            txn.commit();

            log.debug("Commit. Set snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}, " +
                            "persistedSnapshotStart={}",
                    topologyConfigId, ts, persistedTopologyConfigID, persistedSnapshotStart);
        }

        return (ts == getLastStartedSnapshotTimestamp() && topologyConfigId == getTopologyConfigId());
    }


    /**
     * This call should be done in a transaction after a snapshot transfer is complete and before the apply starts.
     *
     * @param topologyConfigId current topology config identifier
     * @param ts timestamp of completed snapshot sync transfer
     */
    public void setLastSnapshotTransferCompleteTimestamp(long topologyConfigId, long ts) {
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            // Read metadata & validate
            Map<LogReplicationMetadataType, Long> metadataMap = queryMetadata(txn, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID,
                    LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
            long persistedTopologyConfigId = metadataMap.get(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
            long persistedSnapshotStart = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);

            log.debug("Update last snapshot transfer completed, topologyConfigId={}, transferCompleteTs={}," +
                            " persistedTopologyConfigID={}, persistedSnapshotStart={}", topologyConfigId, ts,
                    persistedTopologyConfigId, persistedSnapshotStart);

            // It means the cluster config has changed, ignore the update operation.
            if (topologyConfigId != persistedTopologyConfigId || ts < persistedSnapshotStart) {
                log.warn("Metadata mismatch, persisted={}, intended={}. Snapshot Transfer complete timestamp {} " +
                                "will not be persisted, current={}",
                        persistedTopologyConfigId, topologyConfigId, ts, persistedSnapshotStart);
                return;
            }

            // Update the topologyConfigId to fence all other transactions that update the metadata at the same time
            appendUpdate(txn, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
            appendUpdate(txn, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED, ts);
            txn.commit();
        }

        log.debug("Commit snapshot transfer complete timestamp={}, for topologyConfigId={}", ts, topologyConfigId);
    }

    public void setSnapshotAppliedComplete(LogReplication.LogReplicationEntryMsg entry) {
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            Map<LogReplicationMetadataType, Long> metadataMap = queryMetadata(txn, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID,
                    LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED);
            long persistedTopologyConfigId = metadataMap.get(LogReplicationMetadataType.TOPOLOGY_CONFIG_ID);
            long persistedSnapshotStart = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED);
            long persistedSnapshotTransferComplete = metadataMap.get(LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED);
            long topologyConfigId = entry.getMetadata().getTopologyConfigID();
            long ts = entry.getMetadata().getSnapshotTimestamp();

            if (topologyConfigId != persistedTopologyConfigId || ts != persistedSnapshotStart
                    || ts != persistedSnapshotTransferComplete) {
                log.warn("Metadata mismatch, persisted={}, intended={}. Entry timestamp={}, while persisted start={}, transfer={}",
                        persistedTopologyConfigId, topologyConfigId, ts, persistedSnapshotStart, persistedSnapshotTransferComplete);
                return;
            }

            // Update the topologyConfigId to fence all other transactions that update the metadata at the same time
            appendUpdate(txn, LogReplicationMetadataType.TOPOLOGY_CONFIG_ID, topologyConfigId);
            appendUpdate(txn, LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED, ts);
            appendUpdate(txn, LogReplicationMetadataType.LAST_LOG_ENTRY_PROCESSED, ts);
            txn.commit();

            log.debug("Commit snapshot apply complete timestamp={}, for topologyConfigId={}", ts, topologyConfigId);
        }
    }

    /**
     * Update replication status table's snapshot sync info as ongoing.
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param clusterId standby cluster id
     */
    public void updateSnapshotSyncInfo(String clusterId, boolean forced, UUID eventId,
                                       long baseVersion, long remainingEntries) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();

        SnapshotSyncInfo.SnapshotSyncType syncType = forced ?
                SnapshotSyncInfo.SnapshotSyncType.FORCED :
                SnapshotSyncInfo.SnapshotSyncType.DEFAULT;

        SnapshotSyncInfo syncInfo = SnapshotSyncInfo.newBuilder()
                .setType(syncType)
                .setStatus(SyncStatus.ONGOING)
                .setSnapshotRequestId(eventId.toString())
                .setBaseSnapshot(baseVersion)
                .build();

        ReplicationStatusVal status = ReplicationStatusVal.newBuilder()
                .setRemainingEntriesToSend(remainingEntries)
                .setSyncType(ReplicationStatusVal.SyncType.SNAPSHOT)
                .setStatus(SyncStatus.ONGOING)
                .setSnapshotSyncInfo(syncInfo)
                .build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            txn.putRecord(replicationStatusTable, key, status, null);
            txn.commit();
        }

        log.debug("updateSnapshotSyncInfo as ongoing: clusterId: {}, syncInfo: {}",
                clusterId, syncInfo);
    }

    /**
     * Update replication status table's snapshot sync info as completed.
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param clusterId standby cluster id
     */
    public void updateSnapshotSyncInfo(String clusterId) {
        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build();
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {

            CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> record = txn.getRecord(replicationStatusTable, key);

            if (record != null) {
                ReplicationStatusVal previous = record.getPayload();
                SnapshotSyncInfo previousSyncInfo = previous.getSnapshotSyncInfo();

                SnapshotSyncInfo currentSyncInfo = previousSyncInfo.toBuilder()
                        .setStatus(SyncStatus.COMPLETED)
                        .setCompletedTime(timestamp)
                        .build();

                ReplicationStatusVal current = ReplicationStatusVal.newBuilder()
                        .setRemainingEntriesToSend(previous.getRemainingEntriesToSend())
                        .setSyncType(ReplicationStatusVal.SyncType.LOG_ENTRY)
                        .setStatus(SyncStatus.ONGOING)
                        .setSnapshotSyncInfo(currentSyncInfo)
                        .build();

                txn.putRecord(replicationStatusTable, key, current, null);
                txn.commit();

                log.debug("updateSnapshotSyncInfo as completed: clusterId: {}, syncInfo: {}",
                        clusterId, currentSyncInfo);
            }
        }
    }

    /**
     * Update replication status table's sync status
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param clusterId standby cluster id
     */
    public void updateSyncStatus(String clusterId, ReplicationStatusVal.SyncType lastSyncType, SyncStatus status) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {

            CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> record = txn.getRecord(replicationStatusTable, key);

            ReplicationStatusVal previous;
            if (record.getPayload() != null) {
                previous = record.getPayload();
            } else {
                previous = ReplicationStatusVal.newBuilder().build();
            }

            ReplicationStatusVal current;
            if (lastSyncType.equals(ReplicationStatusVal.SyncType.LOG_ENTRY)) {
                current = previous.toBuilder().setStatus(status).build();
            } else {
                SnapshotSyncInfo syncInfo = previous.getSnapshotSyncInfo();
                syncInfo = syncInfo.toBuilder().setStatus(status).build();
                current = previous.toBuilder().setStatus(status).setSnapshotSyncInfo(syncInfo).build();
            }

            txn.putRecord(replicationStatusTable, key, current, null);
            txn.commit();
        }

        log.debug("updateSyncStatus: clusterId: {}, type: {}, status: {}",
                clusterId, lastSyncType, status);
    }

    /**
     * Set replication status table.
     * If the current sync type is log entry sync, keep Snapshot Sync Info.
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param clusterId standby cluster id
     * @param remainingEntries num of remaining entries to send
     * @param type sync type
     */
    public void setReplicationStatusTable(String clusterId, long remainingEntries,
                                          ReplicationStatusVal.SyncType type) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();
        SnapshotSyncInfo syncInfo = null;
        ReplicationStatusVal current;

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> record = txn.getRecord(replicationStatusTable, key);
            if (record.getPayload() != null) {
                ReplicationStatusVal previous = record.getPayload();
                syncInfo = previous.getSnapshotSyncInfo();
            }
            txn.commit();
        }

        if (type == ReplicationStatusVal.SyncType.LOG_ENTRY) {
            if (syncInfo == null){
                log.warn("setReplicationStatusTable during LOG_ENTRY sync, " +
                        "previous status is not present for cluster: {}", clusterId);
                syncInfo = SnapshotSyncInfo.newBuilder().build();
            }

            current = ReplicationStatusVal.newBuilder()
                    .setRemainingEntriesToSend(remainingEntries)
                    .setSyncType(type)
                    .setStatus(SyncStatus.ONGOING)
                    .setSnapshotSyncInfo(syncInfo)
                    .build();

            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(replicationStatusTable, key, current, null);
                txn.commit();
            }
            
            log.debug("setReplicationStatusTable: clusterId: {}, remainingEntries: {}, type: {}, syncInfo: {}",
                    clusterId, remainingEntries, type, syncInfo);
        } else if (type == ReplicationStatusVal.SyncType.SNAPSHOT) {
            SnapshotSyncInfo currentSyncInfo;
            if (syncInfo == null){
                log.warn("setReplicationStatusTable during SNAPSHOT sync, " +
                        "previous status is not present for cluster: {}", clusterId);
                currentSyncInfo = SnapshotSyncInfo.newBuilder().build();
            } else {
                currentSyncInfo = syncInfo.toBuilder()
                        .setStatus(SyncStatus.ONGOING)
                        .build();
            }

            current = ReplicationStatusVal.newBuilder()
                    .setRemainingEntriesToSend(remainingEntries)
                    .setSyncType(type)
                    .setStatus(SyncStatus.ONGOING)
                    .setSnapshotSyncInfo(currentSyncInfo)
                    .build();

            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(replicationStatusTable, key, current, null);
                txn.commit();
            }

            log.debug("setReplicationStatusTable: clusterId: {}, remainingEntries: {}, type: {}, syncInfo: {}",
                    clusterId, remainingEntries, type, currentSyncInfo);
        }
    }

    public Map<String, ReplicationStatusVal> getReplicationRemainingEntries() {
        Map<String, ReplicationStatusVal> replicationStatusMap = new HashMap<>();
        List<CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message>> entries;
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            entries = txn.executeQuery(replicationStatusTable, record -> true);
            txn.commit();
        }

        for (CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> entry : entries) {
            String clusterId = entry.getKey().getClusterId();
            ReplicationStatusVal value = entry.getPayload();
            replicationStatusMap.put(clusterId, value);
            log.debug("getReplicationRemainingEntries: clusterId={}, remainingEntriesToSend={}, " +
                    "syncType={}, is_consistent={}", clusterId, value.getRemainingEntriesToSend(),
                    value.getSyncType(), value.getDataConsistent());
        }

        log.debug("getReplicationRemainingEntries: replicationStatusMap size: {}", replicationStatusMap.size());

        return replicationStatusMap;
    }

    /**
     * set DataConsistent filed in status table on standby side.
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param isConsistent data is consistent or not
     */
    public void setDataConsistentOnStandby(boolean isConsistent) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(localClusterId).build();
        ReplicationStatusVal val = ReplicationStatusVal.newBuilder()
                .setDataConsistent(isConsistent)
                .setStatus(SyncStatus.UNAVAILABLE)
                .build();
        try (TxnContext txn = getTxnContext()) {
            txn.putRecord(replicationStatusTable, key, val, null);
            txn.commit();
        }

        if (log.isTraceEnabled()) {
            log.trace("setDataConsistentOnStandby: localClusterId: {}, isConsistent: {}", localClusterId, isConsistent);
        }
    }

    public Map<String, ReplicationStatusVal> getDataConsistentOnStandby() {
        CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> record;
        ReplicationStatusVal statusVal;

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            record = txn.getRecord(replicationStatusTable, ReplicationStatusKey.newBuilder().setClusterId(localClusterId).build());
            txn.commit();
        }

        // Initially, snapshot sync is pending so the data is not consistent.
        if (record == null) {
            log.warn("No Key for Data Consistent found.  DataConsistent Status is not set.");
            statusVal = ReplicationStatusVal.newBuilder().setDataConsistent(false).build();
        } else {
            statusVal = record.getPayload();
        }
        Map<String, ReplicationStatusVal> dataConsistentMap = new HashMap<>();
        dataConsistentMap.put(localClusterId, statusVal);

        log.debug("getDataConsistentOnStandby: localClusterId: {}, statusVal: {}", localClusterId, statusVal);

        return dataConsistentMap;
    }

    public void resetReplicationStatus() {
        log.info("Reset replication status");
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
                   builder.append(getLastStartedSnapshotTimestamp());
                   break;
                case LAST_SNAPSHOT_TRANSFERRED:
                   builder.append(getLastTransferredSnapshotTimestamp());
                   break;
                case LAST_SNAPSHOT_APPLIED:
                   builder.append(getLastAppliedSnapshotTimestamp());
                   break;
                case LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER:
                   builder.append(getLastSnapshotTransferredSequenceNumber());
                   break;
                case LAST_LOG_ENTRY_PROCESSED:
                   builder.append(getLastProcessedLogEntryTimestamp());
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
    public void setSnapshotSyncStartMarker(TxnContext txn, UUID currentSnapshotSyncId, CorfuStoreMetadata.Timestamp shadowStreamTs) {

        long currentSnapshotSyncIdLong = currentSnapshotSyncId.getMostSignificantBits() & Long.MAX_VALUE;
        long persistedSnapshotId = queryMetadata(txn, LogReplicationMetadataType.CURRENT_SNAPSHOT_CYCLE_ID).get(LogReplicationMetadataType.CURRENT_SNAPSHOT_CYCLE_ID);

        if (persistedSnapshotId != currentSnapshotSyncIdLong) {
            // Update if current Snapshot Sync differs from the persisted one, otherwise ignore.
            // It could have already been updated in the case that leader changed in between a snapshot sync cycle
            appendUpdate(txn, LogReplicationMetadataType.CURRENT_SNAPSHOT_CYCLE_ID, currentSnapshotSyncIdLong);
            appendUpdate(txn, LogReplicationMetadataType.CURRENT_CYCLE_MIN_SHADOW_STREAM_TS, shadowStreamTs.getSequence());
        }
    }

    /**
     * Retrieve the snapshot sync start marker
     **/
    public long getMinSnapshotSyncShadowStreamTs() {
        return queryMetadata(LogReplicationMetadataType.CURRENT_CYCLE_MIN_SHADOW_STREAM_TS);
    }

    /**
     * Retrieve the current snapshot sync cycle Id
     */
    public long getCurrentSnapshotSyncCycleId() {
        return queryMetadata( LogReplicationMetadataType.CURRENT_SNAPSHOT_CYCLE_ID);
    }

    /**
     * Interface to write an event to the logReplicationEventTable.
     * @param key
     * @param event
     */
    public void updateLogReplicationEventTable(ReplicationEventKey key, ReplicationEvent event) {
        log.info("UpdateReplicationEvent {} with event {}", REPLICATION_EVENT_TABLE_NAME, event);
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            txn.putRecord(replicationEventTable, key, event, null);
            txn.commit();
        }
    }

    /**
     * Subscribe to the logReplicationEventTable
     * @param listener
     */
    public void subscribeReplicationEventTable(StreamListener listener) {
        log.info("LogReplication start listener for table {}", REPLICATION_EVENT_TABLE_NAME);
        corfuStore.subscribe(listener, NAMESPACE,
                Collections.singletonList(new TableSchema(REPLICATION_EVENT_TABLE_NAME, ReplicationEventKey.class, ReplicationEvent.class, null)), null);
    }

    /**
     * Unsubscribe the logReplicationEventTable
     * @param listener
     */
    public void unsubscribeReplicationEventTable(StreamListener listener) {
        corfuStore.unsubscribe(listener);
    }

    public void shutdown() {
        // No-Op
    }

    public enum LogReplicationMetadataType {
        TOPOLOGY_CONFIG_ID("topologyConfigId"),
        VERSION("version"),
        LAST_SNAPSHOT_STARTED("lastSnapshotStarted"),
        LAST_SNAPSHOT_TRANSFERRED("lastSnapshotTransferred"),
        LAST_SNAPSHOT_APPLIED("lastSnapshotApplied"),
        LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER("lastSnapshotTransferredSeqNumber"),
        CURRENT_SNAPSHOT_CYCLE_ID("currentSnapshotCycleId"),
        CURRENT_CYCLE_MIN_SHADOW_STREAM_TS("minShadowStreamTimestamp"),
        LAST_LOG_ENTRY_PROCESSED("lastLogEntryProcessed"),
        REMAINING_REPLICATION_PERCENT("replicationStatus"),
        DATA_CONSISTENT_ON_STANDBY("dataConsistentOnStandby"),
        SNAPSHOT_SYNC_TYPE("snapshotSyncType"),
        SNAPSHOT_SYNC_COMPLETE_TIME("snapshotSyncCompleteTime");

        @Getter
        String val;
        LogReplicationMetadataType(String newVal) {
            val  = newVal;
        }
    }
}
