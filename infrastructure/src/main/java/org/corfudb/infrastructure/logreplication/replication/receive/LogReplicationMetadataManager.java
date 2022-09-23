package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventInfoKey;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.LogReplication.ReplicationInfo;
import org.corfudb.runtime.LogReplication.SyncType;
import org.corfudb.runtime.LogReplication.SinkReplicationStatus;
import org.corfudb.runtime.LogReplication.SnapshotSyncInfo;
import org.corfudb.runtime.LogReplication.SnapshotSyncInfo.SnapshotSyncType;
import org.corfudb.runtime.LogReplication.SourceReplicationStatus;
import org.corfudb.runtime.LogReplication.SyncStatus;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * This class manages relevant metadata associated to all ongoing replication sessions for Source and Sink.
 * It maintains 3 tables:
 *
 * (1) Log Replication 'Metadata' Table: holds relevant metadata for a given session.
 *
 * (2) Log Replication 'Status' Table: status of a replication session, this info is mostly intended for client consumption,
 * e.g., User Interface / Alarms system. Note that this data is a subset of metadata information aimed for consumption.
 * Note that, the status is kept on separate tables for source and sink, as status information can only be queried locally
 * and not across clusters.
 *
 * (3) Log Replication 'Event' Table: used to communicate events across lead and non-lead nodes. Currently, the only
 * event supported is 'Force Snapshot Sync"
 *
 * For Source it holds replication status
 * For Sink it holds replication status & replication metadata
 */
@Slf4j
public class LogReplicationMetadataManager {

    public static final String NAMESPACE = CORFU_SYSTEM_NAMESPACE;
    public static final String METADATA_TABLE_PREFIX_NAME = "CORFU-REPLICATION-WRITER-";
    public static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";
    public static final String LR_STATUS_STREAM_TAG = "lr_status";
    public static final String REPLICATION_EVENT_TABLE_NAME = "LogReplicationEventTable";
    public static final String LR_STREAM_TAG = "log_replication";

    private final CorfuStore corfuStore;

    @Getter
    private final CorfuRuntime runtime;

    private final String remoteClusterId;

    private final Table<ReplicationStatusKey, ReplicationStatusVal, Message> replicationStatusTable;
    private final Table<LogReplicationMetadataKey, LogReplicationMetadataVal, Message> metadataTable;
    private final Table<ReplicationEventKey, ReplicationEvent, Message> replicationEventTable;

    private Optional<Timer.Sample> snapshotSyncTimerSample = Optional.empty();

    public LogReplicationMetadataManager(CorfuRuntime rt, long topologyConfigId, String remoteClusterId) {
        this.runtime = rt;
        this.corfuStore = new CorfuStore(runtime);

        metadataTableName = getPersistedWriterMetadataTableName(remoteClusterId);
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

            this.remoteClusterId = remoteClusterId;
        } catch (Exception e) {
            log.error("Caught an exception while opening MetadataManagerTables ", e);
            throw new ReplicationWriterException(e);
        }
        setupTopologyConfigId(topologyConfigId);
    }

    /**
     * A generic way to try open a table and handle any exceptions encountered
     * @param corfuStore - instance of the corfuStore
     * @param namespace - just the namespace part of the full table name
     * @param tableName - just the tablename part of the full table name
     * @param keyClass - Protobuf key class that extends Message
     * @param valueClass - Protobuf generated Value or Payload class that extends Message
     * @param metadataClass - Protobuf generated Metadata class (can be null)
     * @return - a generic table type.
     */
    public static Table tryOpenTable(CorfuStore corfuStore, String namespace, String tableName,
                                     Class keyClass, Class valueClass, Class metadataClass) {
        Table table = null;
        try {
            table = corfuStore.openTable(namespace, tableName,
                    keyClass, valueClass, metadataClass,
                    TableOptions.fromProtoSchema(valueClass));
        } catch (Exception ee) {
            log.error("Caught an exception while opening "+namespace+"$"+tableName, ee);
            throw new ReplicationWriterException(ee);
        }
        return table;
    }

    /**
     * The following tables need to move from old type to new type
     * 1. CORFU-REPLICATION-WRITER-UUID-OF-CLUSTER -> LogReplicationMetadataTable
     * Type: Schema change AND name change
     *    (LogReplicationMetadataKey, LogReplicationMetadataVal) ->
     *     (LogReplicationSession, ReplicationMetadata)
     * Action: Open the old table with old types and Clear the old table
     *         Not used in new code. Optional as failure to clear won't cause issues.
     *         TODO: consider scanning registry for signature and wipe out contents.
     *
     * 2. LogReplicationStatus
     * Type: Schema change for key AND value types with NO table name change
     * (ReplicationStatusKey, ReplicationStatusVal) ->
     * (LogReplicationSession.class, ReplicationMetadata.class)
     * Action: Make serializer aware of old types, open table with new type, clear it
     *
     * 3. LogReplicationEventTable
     * Type: Schema change for Key type only
     * (ReplicationEventKey, ReplicationEvent) ->
     * (ReplicationEventInfoKey, ReplicationEvent)
     * Action: Make serializer aware of old key type, open table with new type, clear it
     * @param corfuStore - the same runtime used by LogReplicationMetadataManager
     */
    public static void addLegacyTypesToSerializer(CorfuStore corfuStore) {
        corfuStore.getRuntime().getTableRegistry()
                .addTypeToClassMap(LogReplicationMetadata.ReplicationStatusKey.getDefaultInstance());
        corfuStore.getRuntime().getTableRegistry()
                .addTypeToClassMap(LogReplicationMetadata.ReplicationStatusVal.getDefaultInstance());
        corfuStore.getRuntime().getTableRegistry()
                .addTypeToClassMap(LogReplicationMetadata.ReplicationEventKey.getDefaultInstance());
    }

    public static void migrateData(TxnContext txnContext) {
        txnContext.clear(txnContext.getTable(LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME));
        txnContext.clear(txnContext.getTable(REPLICATION_EVENT_TABLE_NAME));
    }

    /**
     * Initialize LogReplicationStatus table. Note that it is required to initialize those fields explicitly instead
     * of relying on the default value, as some clients of log replicator v1 consume the status table with hasField
     * check.
     */
    private void initializeMetadata(TxnContext txn, LogReplicationSession session, boolean incomingSession,
                                    long topologyConfigId) {
        if (incomingSession) {
            // Add an entry for this session if it does not exist, otherwise, this is a resuming/ongoing session
            if (!txn.keySet(metadataTable).contains(session)) {

                ReplicationMetadata defaultMetadata = getDefaultMetadata(topologyConfigId);

                log.debug("Adding entry for session={} in Replication Metadata Table", session);
                txn.putRecord(metadataTable, session, defaultMetadata, null);
            }

            if (!txn.keySet(statusTable).contains(session)) {

                ReplicationStatus defaultSinkStatus = ReplicationStatus.newBuilder()
                        .setSinkStatus(SinkReplicationStatus.newBuilder()
                                .setDataConsistent(false)
                                .setReplicationInfo(ReplicationInfo.newBuilder()
                                        .setStatus(SyncStatus.NOT_STARTED)
                                        .setSnapshotSyncInfo(SnapshotSyncInfo.newBuilder()
                                                .setStatus(SyncStatus.NOT_STARTED)
                                                .build())
                                        .build())
                                .build())
                        .build();

                log.debug("Adding entry for session={}[Sink] in Replication Status Table", session);
                txn.putRecord(statusTable, session, defaultSinkStatus, null);
            }
        } else if (!txn.keySet(statusTable).contains(session)) {
            ReplicationStatus defaultSourceStatus = ReplicationStatus.newBuilder()
                    .setSourceStatus(SourceReplicationStatus.newBuilder()
                            .setRemainingEntriesToSend(-1L)
                            .setReplicationInfo(ReplicationInfo.newBuilder()
                                    .setStatus(SyncStatus.NOT_STARTED)
                                    .setSnapshotSyncInfo(SnapshotSyncInfo.newBuilder()
                                            .setStatus(SyncStatus.NOT_STARTED)
                                            .build())
                                    .build())
                            .build())
                    .build();

            log.debug("Adding entry for session={}[Source] in Replication Status Table", session);
            txn.putRecord(statusTable, session, defaultSourceStatus, null);
        }
    }

    public TxnContext getTxnContext() {
        return corfuStore.txn(NAMESPACE);
    }

    private ReplicationMetadata getDefaultMetadata(long topologyConfigId) {
        return ReplicationMetadata.newBuilder()
                .setTopologyConfigId(topologyConfigId)
                .setLastLogEntryApplied(Address.NON_ADDRESS)
                .setLastLogEntryBatchProcessed(Address.NON_ADDRESS)
                .setLastSnapshotTransferredSeqNumber(Address.NON_ADDRESS)
                .setLastSnapshotApplied(Address.NON_ADDRESS)
                .setLastSnapshotTransferred(Address.NON_ADDRESS)
                .setLastSnapshotStarted(Address.NON_ADDRESS)
                .setCurrentCycleMinShadowStreamTs(Address.NON_ADDRESS)
                .build();
    }


    // =========================== Replication Metadata Table Methods ===============================

    /**
     * Get the replication metadata for a given LR session and set it to default values if no metadata is found
     *
     * @param session   unique identifier for LR session
     * @return          replication metadata info
     */
    public ReplicationMetadata getReplicationMetadata(LogReplicationSession session) {
        ReplicationMetadata metadata;

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            metadata = queryReplicationMetadata(txn, session);
            txn.commit();
        }
        return metadata;
    }

    /**
     * Query the replication metadata / status as part of an ongoing transaction, for a given LR session
     *
     * @param txn open/ongoing transaction context
     * @param session unique identifier of LR session
     * @return replication metadata info for given session
     */
    public ReplicationMetadata queryReplicationMetadata(TxnContext txn, LogReplicationSession session) {
        CorfuStoreEntry<LogReplicationSession, ReplicationMetadata, Message> entry = txn.getRecord(METADATA_TABLE_NAME,
            session);
        return entry.getPayload();
    }

    /**
     * Update a single field of replication metadata for a given LR session as part of an ongoing transaction
     *
     * @param txn transaction context, for atomic commit
     * @param session unique identifier for LR Session
     * @param fieldNumber field number corresponding to attribute in replication metadata to be updated
     * @param value value to update
     */
    public void updateReplicationMetadataField(TxnContext txn, LogReplicationSession session, int fieldNumber, Object value) {
        Descriptors.FieldDescriptor fd = ReplicationMetadata.getDescriptor().findFieldByNumber(fieldNumber);
        if (fd == null) {
            log.error("Failed to find metadata field number {} in ReplicationMetadata object. Metadata is not UPDATED!", fieldNumber);
            return;
        }
        CorfuStoreEntry<LogReplicationSession, ReplicationMetadata, Message> entry = txn.getRecord(metadataTable, session);

        if(entry.getPayload() == null) {
            log.warn("Entry not found for session={} - skipping update", session);
            return;
        }
        ReplicationMetadata updatedMetadata = entry.getPayload().toBuilder().setField(fd, value).build();
        txn.putRecord(metadataTable, session, updatedMetadata, null);

        log.debug("Update metadata field {}, value={}, session={}", fd.getFullName(), value, session);
    }

    /**
     * Update a single field of replication metadata for a given LR session
     *
     * @param session       unique identifier for LR Session
     * @param fieldNumber   field number corresponding to attribute in replication metadata to be updated
     * @param value         value to update
     */
    @VisibleForTesting
    public void updateReplicationMetadataField(LogReplicationSession session, int fieldNumber, Object value) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                    updateReplicationMetadataField(txn, session, fieldNumber, value);
                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.error("Error while attempting to update replication metadata", tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to update replication metadata", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Update replication metadata for a given LR session as part of an ongoing transaction.
     *
     * @param txn transaction context, for atomic commit
     * @param session unique identifier for LR Session
     * @param metadata new replication metadata object
     */
    public void updateReplicationMetadata(TxnContext txn, LogReplicationSession session, ReplicationMetadata metadata) {
        txn.putRecord(metadataTable, session, metadata, null);
    }

    /**
     * Add default/initial entries on metadata tables for the given session - in the context of an ongoing transaction
     *
     * @param txn                   the context of an ongoing transaction
     * @param session               the session to add metadata entries
     * @param incoming              true, if session is incoming (sink), false otherwise (source)
     */
    public void addSession(TxnContext txn, LogReplicationSession session, long topologyConfigId, boolean incoming) {
        if(replicationContext.getIsLeader().get()) {
            log.info("Add entry to metadata manager, session={}, config_id={}, incoming={}", session, topologyConfigId, incoming);
            initializeMetadata(txn, session, incoming, topologyConfigId);
        }
    }

    /**
     * Add default/initial entries on metadata tables for the given session
     *
     * @param session               the session to add metadata entries
     * @param topologyConfigId      the initial topology configuration identifier
     * @param incoming              true, if session is incoming (sink), false otherwise (source)
     */
    @VisibleForTesting
    public void addSession(LogReplicationSession session, long topologyConfigId, boolean incoming) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                    addSession(txn, session, topologyConfigId, incoming);
                    txn.commit();
                } catch (TransactionAbortedException e) {
                    log.error("Exception when updating the topology config id", e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when updating the topology config id", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    // =========================== Replication Status Table Methods ===============================

    /**
     * Retrieve replication status for all sessions (incoming and outgoing)
     *
     */
    public Map<LogReplicationSession, ReplicationStatus> getReplicationStatus() {
        Map<LogReplicationSession, ReplicationStatus> statusMap = new HashMap<>();
        List<CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message>> entries;

        try(TxnContext txn = corfuStore.txn(NAMESPACE)) {
            entries = txn.executeQuery(statusTable, e -> true);
            txn.commit();
        }

        if (entries != null) {
            for (CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> entry : entries) {
                statusMap.put(entry.getKey(), entry.getPayload());
            }
        }
        return statusMap;
    }

    // =========================== Replication Status Table Methods ===============================


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
     * @param session unique identifier for LR session
     * @param topologyConfigId current topologyConfigId
     * @param snapshotStartTs snapshot start timestamp
     * @return true, if succeeds
     *         false, otherwise
     */
    public boolean setBaseSnapshotStart(LogReplicationSession session, long topologyConfigId, long snapshotStartTs) {
        if(!replicationContext.getIsLeader().get()) {
            log.debug("The node is not the leader. Skip updating the metadata table");
            return false;
        }

        ReplicationMetadata metadata;

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {

            metadata = queryReplicationMetadata(txn, session);

            log.debug("Set snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}, persistedSnapshotStart={}",
                    topologyConfigId, snapshotStartTs, metadata.getTopologyConfigId(), metadata.getLastSnapshotStarted());

            // It means the cluster config has changed, ignore the update operation.
            if (topologyConfigId != metadata.getTopologyConfigId()) {
                log.warn("Config differs between source and sink, Source[topologyConfigId={}, ts={}]" +
                        " Sink[topologyConfigId={}, snapshotStart={}]", topologyConfigId,
                        snapshotStartTs, metadata.getTopologyConfigId(), metadata.getLastSnapshotStarted());
                txn.commit();
                return false;
            }

            ReplicationMetadata updatedMetadata = metadata.toBuilder()
                    .setTopologyConfigId(topologyConfigId)  // Update the topologyConfigId to fence all other txs
                    // that update the metadata at the same time
                    .setLastSnapshotStarted(snapshotStartTs)
                    .setLastSnapshotTransferred(Address.NON_ADDRESS) // Reset other metadata fields
                    .setLastSnapshotApplied(Address.NON_ADDRESS)
                    .setLastSnapshotTransferredSeqNumber(Address.NON_ADDRESS)
                    .setLastLogEntryApplied(Address.NON_ADDRESS)
                    .build();

            updateReplicationMetadata(txn, session, updatedMetadata);
            txn.commit();

            log.debug("Commit. Set snapshotStart topologyConfigId={}, ts={}, persistedTopologyConfigID={}, " +
                    "persistedSnapshotStart={}", topologyConfigId, snapshotStartTs, metadata.getTopologyConfigId(),
                    metadata.getLastSnapshotStarted());
        }
        return true;
    }

    /**
     * This call should be done in a transaction after a snapshot transfer is complete and before the apply starts.
     *
     * @param topologyConfigId current topology config identifier
     * @param ts timestamp of completed snapshot sync transfer
     */
    public void setLastSnapshotTransferCompleteTimestamp(LogReplicationSession session, long topologyConfigId, long ts) {
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            ReplicationMetadata metadata = queryReplicationMetadata(txn, session);
            long persistedTopologyConfigId = metadata.getTopologyConfigId();
            long persistedSnapshotStart = metadata.getLastSnapshotStarted();

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

            ReplicationMetadata updatedMetadata = metadata.toBuilder()
                    .setLastSnapshotTransferred(ts)
                    .setTopologyConfigId(topologyConfigId)
                    .build();

            // Update the topologyConfigId to fence all other transactions that update the metadata at the same time
            updateReplicationMetadata(txn, session, updatedMetadata);
            txn.commit();
        }

        log.debug("Commit snapshot transfer complete timestamp={}, for topologyConfigId={}", ts, topologyConfigId);
    }

    public void setSnapshotAppliedComplete(LogReplicationEntryMsg entry, LogReplicationSession session) {
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {

            ReplicationMetadata metadata = queryReplicationMetadata(txn, session);

            long persistedSnapshotStart = metadata.getLastSnapshotStarted();
            long persistedSnapshotTransferComplete = metadata.getLastSnapshotTransferred();
            long topologyConfigId = entry.getMetadata().getTopologyConfigID();
            long ts = entry.getMetadata().getSnapshotTimestamp();

            if (topologyConfigId != metadata.getTopologyConfigId() || ts != persistedSnapshotStart
                    || ts != persistedSnapshotTransferComplete) {
                log.warn("Metadata mismatch, persisted={}, intended={}. Entry timestamp={}, while persisted start={}, transfer={}",
                        metadata.getTopologyConfigId(), topologyConfigId, ts, persistedSnapshotStart, persistedSnapshotTransferComplete);
                return;
            }

            // Update the topologyConfigId to fence all other transactions that update the metadata at the same time
            ReplicationMetadata updatedMetadata = metadata.toBuilder()
                    .setLastSnapshotApplied(ts)
                    .setLastLogEntryBatchProcessed(ts)
                    .build();

            updateReplicationMetadata(txn, session, updatedMetadata);

            // Set 'isDataConsistent' flag on replication status table atomically with snapshot sync completed
            // information, to prevent any inconsistency between flag and state of snapshot sync completion in
            // the event of crashes
            setDataConsistentOnSink(true, txn);
            txn.commit();

            log.debug("Commit snapshot apply complete timestamp={}, for topologyConfigId={}", ts, topologyConfigId);
        }
    }

    /**
     * Set the snapshot sync start marker, i.e., a unique identification of the current snapshot sync cycle.
     * Identified by the snapshot sync Id and the min shadow stream update timestamp for this cycle.
     * @param txn
     * @param session
     * @param newSnapshotCycleId
     * @param shadowStreamTs
     */
    public void setSnapshotSyncStartMarker(TxnContext txn, LogReplicationSession session, UUID newSnapshotCycleId,
                                           CorfuStoreMetadata.Timestamp shadowStreamTs) {
        ReplicationMetadata metadata = queryReplicationMetadata(txn, session);


        UUID currentSnapshotCycleId = new UUID(metadata.getCurrentSnapshotCycleId().getMsb(), metadata.getCurrentSnapshotCycleId().getLsb());

        // Update if current Snapshot Sync differs from the persisted one, otherwise ignore.
        // It could have already been updated in the case that leader changed in between a snapshot sync cycle
        if (!Objects.equals(currentSnapshotCycleId, newSnapshotCycleId)) {
            RpcCommon.UuidMsg uuidMsg = RpcCommon.UuidMsg.newBuilder()
                .setMsb(newSnapshotCycleId.getMostSignificantBits())
                .setLsb(newSnapshotCycleId.getLeastSignificantBits())
                .build();

            ReplicationMetadata updatedMetadata = metadata.toBuilder()
                .setCurrentCycleMinShadowStreamTs(shadowStreamTs.getSequence())
                .setCurrentSnapshotCycleId(uuidMsg)
                .build();

            updateReplicationMetadata(txn, session, updatedMetadata);
        }
    }

    // =============================== Replication Event Table Methods ===================================

    /**
     * Add log replication event
     *
     * Because a ReplicationEvent can be triggered from a lead or non-lead node, we persist it in CorfuDB
     * for lead node to process accordingly.
     *
     * @param key
     * @param event
     */
    public void addEvent(ReplicationEventInfoKey key, ReplicationEvent event) {
        log.info("Add event :: {}", event);
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            txn.putRecord(replicationEventTable, key, event, null);
            txn.commit();
        }
    }

    /**
     * Get all replication events from the Event table.
     * @return list of all replication events
     */
    public List<CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message>> getReplicationEvents() {
        List<CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message>> events = new ArrayList<>();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
             events = txn.executeQuery(REPLICATION_EVENT_TABLE_NAME, p -> true);
             txn.commit();
        } catch (Exception e) {
            log.error("Failed to get the replication events", e);
        }
        return events;
    }

    // ================================= Replication Status Table Methods ===================================

    /**
     * Update replication status table's snapshot sync info as ongoing.
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param clusterId sink cluster id
     */
    public void updateSnapshotSyncStatusOngoing(LogReplicationSession session, boolean forced, UUID eventId,
                                                long baseVersion, long remainingEntries) {

        SnapshotSyncType snapshotSyncType = forced ? SnapshotSyncType.FORCED : SnapshotSyncType.DEFAULT;

        SnapshotSyncInfo syncInfo = SnapshotSyncInfo.newBuilder()
                .setType(snapshotSyncType)
                .setStatus(SyncStatus.ONGOING)
                .setSnapshotRequestId(eventId.toString())
                .setBaseSnapshot(baseVersion)
                .build();

        SourceReplicationStatus sourceStatus = SourceReplicationStatus.newBuilder()
                .setRemainingEntriesToSend(remainingEntries)
                .setReplicationInfo(ReplicationInfo.newBuilder()
                        .setSyncType(SyncType.SNAPSHOT)
                        .setStatus(SyncStatus.ONGOING)
                        .setSnapshotSyncInfo(syncInfo)
                        .build())
                .build();

        ReplicationStatus status = ReplicationStatus.newBuilder()
                .setSourceStatus(sourceStatus)
                .build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            txn.putRecord(statusTable, session, status, null);
            txn.commit();
        }

        // Start the timer for log replication snapshot sync duration metrics.
        snapshotSyncTimerSample = MeterRegistryProvider.getInstance().map(Timer::start);

        log.debug("syncStatus :: set snapshot sync status to ONGOING, session: {}, syncInfo: [{}]",
                session, syncInfo);
    }

    /**
     * Update replication status table's snapshot sync info as COMPLETED
     * and update log entry sync status to ONGOING.
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param clusterId sink cluster id
     */
    public void updateSnapshotSyncStatusCompleted(LogReplicationSession session, long remainingEntriesToSend,
                                                  long baseSnapshot) {
        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {

            CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> entry = txn.getRecord(statusTable,
                session);

            if (entry.getPayload() != null) {
                SourceReplicationStatus previous = entry.getPayload().getSourceStatus();
                SnapshotSyncInfo previousSyncInfo = previous.getReplicationInfo().getSnapshotSyncInfo();

                SnapshotSyncInfo currentSyncInfo = previousSyncInfo.toBuilder()
                        .setStatus(SyncStatus.COMPLETED)
                        .setBaseSnapshot(baseSnapshot)
                        .setCompletedTime(timestamp)
                        .build();

                ReplicationStatus current = ReplicationStatus.newBuilder()
                        .setSourceStatus(SourceReplicationStatus.newBuilder()
                                .setRemainingEntriesToSend(remainingEntriesToSend)
                                .setReplicationInfo(ReplicationInfo.newBuilder()
                                        .setSyncType(SyncType.LOG_ENTRY)
                                        .setStatus(SyncStatus.ONGOING)
                                        .setSnapshotSyncInfo(currentSyncInfo)
                                        .build())
                                .build())
                        .build();

                txn.putRecord(statusTable, session, current, null);

                log.debug("syncStatus :: set snapshot sync to COMPLETED and log entry ONGOING, session: {}," +
                        " syncInfo: [{}]", session, currentSyncInfo);
            }

            txn.commit();

            snapshotSyncTimerSample
                    .flatMap(sample -> MeterRegistryProvider.getInstance()
                            .map(registry -> {
                                Timer timer = registry.timer("logreplication.snapshot.duration");
                                return sample.stop(timer);
                            }));
        }
    }

    /**
     * Update replication status table's sync status
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param clusterId sink cluster id
     */
    public void updateSyncStatus(LogReplicationSession session, SyncType lastSyncType, SyncStatus status) {

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {

            CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> entry = txn.getRecord(statusTable,
                session);

            // When a remote cluster has been removed from topology, the corresponding entry in the status table is
            // removed and FSM is shutdown. Since FSM shutdown is async, we ensure that we don't update a record which
            // has already been deleted.
            // (STOPPED status is used for other FSM states as well, so cannot rely only on the incoming status)
            if (entry.getPayload() == null && status == SyncStatus.STOPPED) {
                log.debug("syncStatus :: ignoring update for session {} to syncType {} and status {} as no record " +
                        "exists for the same", session, lastSyncType, status);
                txn.commit();
                return;
            }

            SourceReplicationStatus previous = entry.getPayload() != null ? entry.getPayload().getSourceStatus() :
                    SourceReplicationStatus.newBuilder().build();
            SourceReplicationStatus current;

            if (lastSyncType.equals(SyncType.LOG_ENTRY)) {
                current = previous.toBuilder()
                        .setReplicationInfo(previous.getReplicationInfo().toBuilder()
                                .setSyncType(SyncType.LOG_ENTRY)
                                .setStatus(status)
                                .build())
                        .build();
            } else {
                SnapshotSyncInfo syncInfo = previous.getReplicationInfo().getSnapshotSyncInfo();
                syncInfo = syncInfo.toBuilder().setStatus(status).build();
                current = previous.toBuilder()
                        .setReplicationInfo(previous.getReplicationInfo().toBuilder()
                                .setSyncType(SyncType.SNAPSHOT)
                                .setStatus(status)
                                .setSnapshotSyncInfo(syncInfo)
                                .build())
                        .build();
            }

            txn.putRecord(statusTable, session, ReplicationStatus.newBuilder().setSourceStatus(current).build(), null);
            txn.commit();
        }

        log.debug("syncStatus :: Update, session: {}, type: {}, status: {}", session, lastSyncType, status);
    }

    /**
     * Updates the number of remaining entries.
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param clusterId sink cluster id
     * @param remainingEntries num of remaining entries to send
     * @param type sync type
     */
    public void updateRemainingEntriesToSend(LogReplicationSession session, long remainingEntries, SyncType type) {
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> entry = txn.getRecord(statusTable,
                    session);

            ReplicationStatus previous = entry.getPayload();
            SnapshotSyncInfo previousSnapshotSyncInfo = previous.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo();

            if (previous.getSourceStatus().getReplicationInfo().getStatus().equals(SyncStatus.NOT_STARTED) &&
                    previousSnapshotSyncInfo.getStatus().equals(SyncStatus.NOT_STARTED) ||
                    (previous.getSourceStatus().getReplicationInfo().getStatus().equals(SyncStatus.STOPPED) ||
                            previousSnapshotSyncInfo.getStatus().equals(SyncStatus.STOPPED))) {
                // Skip update of sync status, it will be updated once replication is resumed or started
                log.info("syncStatusPoller :: skip remaining entries update, replication status is {}",
                        previous.getSourceStatus().getReplicationInfo().getStatus());
                txn.commit();
                return;
            }

            ReplicationStatus current = previous.toBuilder()
                    .setSourceStatus(previous.getSourceStatus().toBuilder()
                            .setRemainingEntriesToSend(remainingEntries))
                    .build();

            txn.putRecord(statusTable, session, current, null);
            txn.commit();

            log.debug("syncStatusPoller :: remaining entries updated for {}, session: {}, remainingEntries: {}" +
                    "snapshotSyncInfo: {}", type, session, remainingEntries, previousSnapshotSyncInfo);
        }
    }

    // Note: This class is currently instantiated per remote cluster.  In a
    // subsequent PR, change to share a single instance for all remote
    // clusters will be added.  This method returns the replication status
    // for all remote clusters.
    public Map<String, ReplicationStatusVal> getReplicationStatus() {

        Map<String, ReplicationStatusVal> replicationStatusMap = new HashMap<>();
        List<CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message>> entries;

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            entries = txn.executeQuery(statusTable, p -> true);
            txn.commit();
        }

        for (CorfuStoreEntry<ReplicationStatusKey, ReplicationStatusVal, Message> entry : entries) {
            String clusterId = entry.getKey().getClusterId();
            ReplicationStatusVal value = entry.getPayload();
            replicationStatusMap.put(clusterId, value);
            log.debug("getReplicationStatus: clusterId={}, remainingEntriesToSend={}, syncType={}, is_consistent={}",
                clusterId, value.getRemainingEntriesToSend(), value.getSyncType(), value.getDataConsistent());
        }
        return replicationStatusMap;
    }

    /**
     * Set DataConsistent field in status table on sink side.
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param isConsistent data is consistent or not
     * @param session log replication session identifier
     */
    public void setDataConsistentOnSink(boolean isConsistent) {
        ReplicationStatusKey key =
            ReplicationStatusKey.newBuilder().setClusterId(remoteClusterId).build();
        ReplicationStatusVal val = ReplicationStatusVal.newBuilder()
                .setDataConsistent(isConsistent)
                .setStatus(SyncStatus.UNAVAILABLE)
                .build();
        try (TxnContext txn = getTxnContext()) {
            ReplicationStatus status = txn.getRecord(statusTable, session).getPayload();
            SinkReplicationStatus sinkReplicationStatus = status.getSinkStatus();
            status.toBuilder().setSinkStatus(
                    sinkReplicationStatus.toBuilder().setDataConsistent(isConsistent).build()
            ).build();

            txn.putRecord(statusTable, session, status, null);
            txn.commit();
        }

        log.debug("setDataConsistentOnSink: remoteClusterId: {}, " +
            "isConsistent: {}", remoteClusterId, isConsistent);
    }

    public void setDataConsistentOnSink(boolean isConsistent, TxnContext txn) {
        ReplicationStatusKey key =
            ReplicationStatusKey.newBuilder().setClusterId(remoteClusterId).build();
        ReplicationStatusVal val = ReplicationStatusVal.newBuilder()
            .setDataConsistent(isConsistent)
            .setStatus(SyncStatus.UNAVAILABLE)
            .build();
        txn.putRecord(replicationStatusTable, key, val, null);
    }

    /**
     * Reset replication status for all sessions
     */
    public void resetReplicationStatus() {
        log.info("syncStatus :: reset replication status");
        try (TxnContext tx = corfuStore.txn(NAMESPACE)) {
            tx.clear(REPLICATION_STATUS_TABLE);
            tx.commit();
        }
    }

    // Initialize the ReplicationStatus table with a default entry depending on the role.
    // If the role is Source, the default sync type=LOG_ENTRY and status=NOT_STARTED.
    // If the role is Sink and no record for the remote cluster is found, isDataConsistent=false.
    public void initReplicationStatus(boolean isSource) {
        ReplicationStatusKey key = ReplicationStatusKey.newBuilder().setClusterId(remoteClusterId).build();
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            ReplicationStatusVal currentVal =
                (ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();

            if (currentVal == null) {
                currentVal = ReplicationStatusVal.newBuilder().build();
            }

            ReplicationStatusVal newVal = currentVal;
            if (isSource) {
                newVal = currentVal.toBuilder().setSyncType(SyncType.LOG_ENTRY).setStatus(SyncStatus.NOT_STARTED).build();
            }
            txn.putRecord(replicationStatusTable, key, newVal, null);
            txn.commit();
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
                case LAST_LOG_ENTRY_BATCH_PROCESSED:
                   builder.append(getLastProcessedLogEntryBatchTimestamp());
                   break;
                case LAST_LOG_ENTRY_APPLIED:
                    builder.append(getLastAppliedLogEntryTimestamp());
                default:
                    // error
            }
        }
        builder.append("Replication Status: ");
        Map<String, ReplicationStatusVal> replicationStatusMap = getReplicationStatus();

        for (Map.Entry<String, ReplicationStatusVal> entry :
            replicationStatusMap.entrySet()) {
            builder.append(entry.getKey())
                .append("Remaining Entries to Send(if source): ")
                .append(entry.getValue().getRemainingEntriesToSend())
                .append("Data Consistent(if sink):")
                .append(entry.getValue().getDataConsistent());
        }
        return builder.toString();
    }

    // ================================ Runtime Helper Functions ======================================

    public long getLogHead() {
        return runtime.getAddressSpaceView().getTrimMark().getSequence();
    }

    // ================================ End Runtime Helper Functions ==================================

    /**
     * Reset manager by clearing all tables
     */
    public void reset() {
        if(replicationContext.getIsLeader().get()) {
            log.info("Reset all metadata manager tables");
            try (TxnContext tx = corfuStore.txn(NAMESPACE)) {
                statusTable.clearAll();
                metadataTable.clearAll();
                replicationEventTable.clearAll();
                tx.commit();
            }
        }
    }

    public void removeSession(TxnContext txn, LogReplicationSession session) {
        if(replicationContext.getIsLeader().get()) {
            txn.delete(statusTable, session);
            txn.delete(metadataTable, session);
        }
        log.debug("successfully deleted clusterID {} from {}", clusterId, REPLICATION_STATUS_TABLE);
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
        LAST_LOG_ENTRY_BATCH_PROCESSED("lastLogEntryProcessed"),
        REMAINING_REPLICATION_PERCENT("replicationStatus"),
        DATA_CONSISTENT_ON_SINK("dataConsistentOnSink"),
        SNAPSHOT_SYNC_TYPE("snapshotSyncType"),
        SNAPSHOT_SYNC_COMPLETE_TIME("snapshotSyncCompleteTime"),
        LAST_LOG_ENTRY_APPLIED("lastLongEntryApplied");

        @Getter
        String val;
        LogReplicationMetadataType(String newVal) {
            val  = newVal;
        }
    }
}
