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
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal.SyncType;
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
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

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
    public static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";
    public static final String LR_STATUS_STREAM_TAG = "lr_status";
    private static final String REPLICATION_EVENT_TABLE_NAME = "LogReplicationEventTable";
    private static final String LR_STREAM_TAG = "log_replication";

    private final CorfuStore corfuStore;

    private final String metadataTableName;

    private final CorfuRuntime runtime;
    private final String localClusterId;

    private final Table<ReplicationStatusKey, ReplicationStatusVal, Message> replicationStatusTable;
    private final Table<LogReplicationMetadataKey, LogReplicationMetadataVal, Message> metadataTable;
    private final Table<ReplicationEventKey, ReplicationEvent, Message> replicationEventTable;

    public LogReplicationMetadataManager(CorfuRuntime rt, long topologyConfigId, String localClusterId) {
        this.runtime = rt;
        this.corfuStore = new CorfuStore(runtime);

        metadataTableName = getPersistedWriterMetadataTableName(localClusterId);
        try {
            this.metadataTable = this.corfuStore.openTable(NAMESPACE,
                            metadataTableName,
                            LogReplicationMetadataKey.class,
                            LogReplicationMetadataVal.class,
                            null,
                            TableOptions.fromProtoSchema(LogReplicationMetadataVal.class));

            this.replicationStatusTable = this.corfuStore.openTable(NAMESPACE,
                            REPLICATION_STATUS_TABLE,
                            ReplicationStatusKey.class,
                            ReplicationStatusVal.class,
                            null,
                            TableOptions.fromProtoSchema(ReplicationStatusVal.class));

            this.replicationEventTable = this.corfuStore.openTable(NAMESPACE,
                    REPLICATION_EVENT_TABLE_NAME,
                    ReplicationEventKey.class,
                    ReplicationEvent.class,
                    null,
                    TableOptions.fromProtoSchema(ReplicationEvent.class));

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

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                    for (LogReplicationMetadataType type : LogReplicationMetadataType.values()) {
                        long val = Address.NON_ADDRESS;
                        if (type == LogReplicationMetadataType.TOPOLOGY_CONFIG_ID) {
                            val = topologyConfigId;
                        }
                        appendUpdate(txn, type, val);
                    }
                    txn.commit();
                } catch (TransactionAbortedException e) {
                    log.error("Exception when updating the topology config id",
                        e);
                    throw new RetryNeededException();
                }
                log.info("Update topologyConfigId, new metadata {}", this);
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when updating the topology " +
                "config id", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
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

            // Set 'isDataConsistent' flag on replication status table atomically with snapshot sync completed
            // information, to prevent any inconsistency between flag and state of snapshot sync completion in
            // the event of crashes
            ReplicationStatusVal statusValue = ReplicationStatusVal.newBuilder()
                    .setDataConsistent(true)
                    .setStatus(SyncStatus.UNAVAILABLE)
                    .build();
            txn.putRecord(replicationStatusTable, ReplicationStatusKey.newBuilder().setClusterId(localClusterId).build(),
                    statusValue, null);

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
    public void updateSnapshotSyncStatusOngoing(String clusterId, boolean forced, UUID eventId,
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
                .setSyncType(SyncType.SNAPSHOT)
                .setStatus(SyncStatus.ONGOING)
                .setSnapshotSyncInfo(syncInfo)
                .build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            txn.putRecord(replicationStatusTable, key, status, null);
            txn.commit();
        }

        log.debug("syncStatus :: set snapshot sync status to ONGOING, clusterId: {}, syncInfo: [{}]",
                clusterId, syncInfo);
    }

    /**
     * Update replication status table's snapshot sync info as COMPLETED
     * and update log entry sync status to ONGOING.
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param clusterId standby cluster id
     */
    public void updateSnapshotSyncStatusCompleted(String clusterId, long remainingEntriesToSend, long baseSnapshot) {
        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build();
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {

            CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> record = txn.getRecord(replicationStatusTable, key);

            if (record.getPayload() != null) {
                ReplicationStatusVal previous = record.getPayload();
                SnapshotSyncInfo previousSyncInfo = previous.getSnapshotSyncInfo();

                SnapshotSyncInfo currentSyncInfo = previousSyncInfo.toBuilder()
                        .setStatus(SyncStatus.COMPLETED)
                        .setBaseSnapshot(baseSnapshot)
                        .setCompletedTime(timestamp)
                        .build();

                ReplicationStatusVal current = ReplicationStatusVal.newBuilder()
                        .setRemainingEntriesToSend(remainingEntriesToSend)
                        .setSyncType(SyncType.LOG_ENTRY)
                        .setStatus(SyncStatus.ONGOING)
                        .setSnapshotSyncInfo(currentSyncInfo)
                        .build();

                txn.putRecord(replicationStatusTable, key, current, null);
                txn.commit();

                log.debug("syncStatus :: set snapshot sync to COMPLETED and log entry ONGOING, clusterId: {}," +
                                " syncInfo: [{}]", clusterId, currentSyncInfo);
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
    public void updateSyncStatus(String clusterId, SyncType lastSyncType, SyncStatus status) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {

            CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> record = txn.getRecord(replicationStatusTable, key);

            ReplicationStatusVal previous = record.getPayload() != null ? record.getPayload() : ReplicationStatusVal.newBuilder().build();
            ReplicationStatusVal current;

            if (lastSyncType.equals(SyncType.LOG_ENTRY)) {
                current = previous.toBuilder().setSyncType(SyncType.LOG_ENTRY).setStatus(status).build();
            } else {
                SnapshotSyncInfo syncInfo = previous.getSnapshotSyncInfo();
                syncInfo = syncInfo.toBuilder().setStatus(status).build();
                current = previous.toBuilder().setSyncType(SyncType.SNAPSHOT).setStatus(status).setSnapshotSyncInfo(syncInfo).build();
            }

            txn.putRecord(replicationStatusTable, key, current, null);
            txn.commit();
        }

        log.debug("syncStatus :: Update, clusterId: {}, type: {}, status: {}", clusterId, lastSyncType, status);
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
    public void setReplicationStatusTable(String clusterId, long remainingEntries, SyncType type) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(clusterId).build();
        SnapshotSyncInfo snapshotStatus = null;
        ReplicationStatusVal current;
        ReplicationStatusVal previous = null;

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> record = txn.getRecord(replicationStatusTable, key);
            if (record.getPayload() != null) {
                previous = record.getPayload();
                snapshotStatus = previous.getSnapshotSyncInfo();
            }
            txn.commit();
        }

        if (type == SyncType.LOG_ENTRY) {
            if (previous != null &&
                    (previous.getStatus().equals(SyncStatus.NOT_STARTED)
                            || snapshotStatus.getStatus().equals(SyncStatus.STOPPED))) {
                log.info("syncStatusPoller :: skip replication status update, log entry replication is {}", previous.getStatus());
                // Skip update of sync status, it will be updated once replication is resumed or started
                return;
            }

            if (snapshotStatus == null){
                log.warn("syncStatusPoller [logEntry]:: previous snapshot status is not present for cluster: {}", clusterId);
                snapshotStatus = SnapshotSyncInfo.newBuilder().build();
            }

            current = ReplicationStatusVal.newBuilder()
                    .setRemainingEntriesToSend(remainingEntries)
                    .setSyncType(type)
                    .setStatus(SyncStatus.ONGOING)
                    .setSnapshotSyncInfo(snapshotStatus)
                    .build();

            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(replicationStatusTable, key, current, null);
                txn.commit();
            }
            
            log.debug("syncStatusPoller :: Log Entry status set to ONGOING, clusterId: {}, remainingEntries: {}, " +
                            "snapshotSyncInfo: {}", clusterId, remainingEntries, snapshotStatus);
        } else if (type == SyncType.SNAPSHOT) {

            SnapshotSyncInfo currentSnapshotSyncInfo;
            if (snapshotStatus == null){
                log.warn("syncStatusPoller [snapshot] :: previous status is not present for cluster: {}", clusterId);
                currentSnapshotSyncInfo = SnapshotSyncInfo.newBuilder().build();
            } else {

                if (snapshotStatus.getStatus().equals(SyncStatus.NOT_STARTED)
                        || snapshotStatus.getStatus().equals(SyncStatus.STOPPED)) {
                    // Skip update of sync status, it will be updated once replication is resumed or started
                    log.info("syncStatusPoller :: skip replication status update, snapshot sync is {}", snapshotStatus);
                    return;
                }

                currentSnapshotSyncInfo = snapshotStatus.toBuilder()
                        .setStatus(SyncStatus.ONGOING)
                        .build();
            }

            current = ReplicationStatusVal.newBuilder()
                    .setRemainingEntriesToSend(remainingEntries)
                    .setSyncType(type)
                    .setStatus(SyncStatus.ONGOING)
                    .setSnapshotSyncInfo(currentSnapshotSyncInfo)
                    .build();

            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(replicationStatusTable, key, current, null);
                txn.commit();
            }

            log.debug("syncStatusPoller :: sync status for {} set to ONGOING, clusterId: {}, remainingEntries: {}",
                    type, clusterId, remainingEntries);
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
     * Set DataConsistent field in status table on standby side.
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

        log.debug("setDataConsistentOnStandby: localClusterId: {}, isConsistent: {}", localClusterId, isConsistent);
    }

    public Map<String, ReplicationStatusVal> getDataConsistentOnStandby() {
        CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> record;
        ReplicationStatusVal statusVal;

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            record = txn.getRecord(replicationStatusTable, ReplicationStatusKey.newBuilder().setClusterId(localClusterId).build());
            txn.commit();
        }

        // Initially, snapshot sync is pending so the data is not consistent.
        if (record.getPayload() == null) {
            log.warn("DataConsistent status is not set for local cluster {}", localClusterId);
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
        log.info("syncStatus :: reset replication status");
        try (TxnContext tx = corfuStore.txn(replicationStatusTable.getNamespace())) {
            replicationStatusTable.clearAll();
            tx.commit();
        }
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
        return queryMetadata(LogReplicationMetadataType.CURRENT_SNAPSHOT_CYCLE_ID);
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
        corfuStore.subscribeListener(listener, NAMESPACE, LR_STREAM_TAG, Collections.singletonList(REPLICATION_EVENT_TABLE_NAME));
    }

    /**
     * Unsubscribe the logReplicationEventTable
     * @param listener
     */
    public void unsubscribeReplicationEventTable(StreamListener listener) {
        corfuStore.unsubscribeListener(listener);
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
