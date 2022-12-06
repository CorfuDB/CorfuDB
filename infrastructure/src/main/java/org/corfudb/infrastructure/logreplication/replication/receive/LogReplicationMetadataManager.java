package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatus;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SyncType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SinkReplicationStatus;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SourceReplicationStatus;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventKey;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SnapshotSyncInfo;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SyncStatus;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.StreamListener;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * The LogReplicationMetadataManager holds relevant metadata associated to
 * all ongoing replication sessions for Source and Sink.
 *
 * It maintains 3 tables:
 *
 * (1) Log Replication 'Metadata' Table: holds relevant metadata for a given session.
 *
 * (2) Log Replication 'Status' Table: status of a replication session, this info is mostly intended for client consumption,
 * e.g., User Interface / Alarms system. Note that this data is a subset of metadata information aimed for consumption.
 * Note that, the status is kept on separate tables for source and sink, as status information can only be queried locally
 * and not across clusters.
 *
 * (3) Log Replication 'Event' Table:
 */
@Slf4j
public class LogReplicationMetadataManager {

    public static final String NAMESPACE = CORFU_SYSTEM_NAMESPACE;
    public static final String METADATA_TABLE_NAME = "LogReplicationMetadataTable";
    public static final String REPLICATION_STATUS_TABLE_NAME_SOURCE = "LogReplicationStatusSource";
    public static final String REPLICATION_STATUS_TABLE_NAME_SINK = "LogReplicationStatusSink";
    public static final String LR_STATUS_STREAM_TAG = "lr_status";
    public static final String REPLICATION_EVENT_TABLE_NAME = "LogReplicationEventTable";
    public static final String LR_STREAM_TAG = "log_replication";

    private final String localClusterId;
    private final CorfuStore corfuStore;

    @Getter
    private final CorfuRuntime runtime;

    private Set<LogReplicationSession> incomingSessions;
    private Set<LogReplicationSession> outgoingSessions;

    private final Table<LogReplicationSession, ReplicationStatus, Message> statusTable;
    private final Table<LogReplicationSession, ReplicationMetadata, Message> metadataTable;
    private final Table<ReplicationEventKey, ReplicationEvent, Message> replicationEventTable;

    private Optional<Timer.Sample> snapshotSyncTimerSample = Optional.empty();

    /**
     * Constructor
     *
     * @param runtime
     */
    public LogReplicationMetadataManager(CorfuRuntime runtime, String localClusterId) {
        this.runtime = runtime;
        this.corfuStore = new CorfuStore(runtime);
        this.localClusterId = localClusterId;

        try {
            this.metadataTable = this.corfuStore.openTable(NAMESPACE, METADATA_TABLE_NAME,
                    LogReplicationSession.class, ReplicationMetadata.class, null,
                    TableOptions.fromProtoSchema(ReplicationMetadata.class));

            this.statusTable = this.corfuStore.openTable(NAMESPACE, REPLICATION_STATUS_TABLE_NAME_SOURCE,
                    LogReplicationSession.class, ReplicationStatus.class, null,
                    TableOptions.fromProtoSchema(ReplicationStatus.class));

            this.replicationEventTable = this.corfuStore.openTable(NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                    ReplicationEventKey.class,
                    ReplicationEvent.class,
                    null,
                    TableOptions.fromProtoSchema(ReplicationEvent.class));
        } catch (Exception e) {
            log.error("Caught an exception while opening metadata tables", e);
            throw new ReplicationWriterException(e);
        }
    }
    
    public LogReplicationMetadataManager(CorfuRuntime rt, Set<LogReplicationSession> sessions, long topologyConfigId,
                                         String localClusterId, String logReplicatorVersion) {
        this(rt, localClusterId);
        processSessions(sessions);
        initializeMetadataTables(topologyConfigId, logReplicatorVersion);
    }

    private void processSessions(Set<LogReplicationSession> sessions) {
        for (LogReplicationSession session : sessions) {
            if (session.getSourceClusterId() == localClusterId && !outgoingSessions.contains(session)) {
                outgoingSessions.add(session);
            } else if (session.getSinkClusterId() == localClusterId && !incomingSessions.contains(session)) {
                incomingSessions.add(session);
            }
        }
    }

    /**
     * Retrieve all sessions for which this cluster is the sink
     *
     * @return set of sessions for which this cluster is the sink
     */
    public Set<LogReplicationSession> getIncomingSessions() {
        return incomingSessions;
    }

    /**
     * Retrieve all sessions for which this cluster is the source
     *
     * @return set of sessions for which this cluster is the sink
     */
    public Set<LogReplicationSession> getOutgoingSessions() {
        return outgoingSessions;
    }

    private void initializeMetadataTables(long topologyConfigId, String logReplicatorVersion) {
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {

            ReplicationMetadata defaultMetadata = ReplicationMetadata.newBuilder()
                    .setTopologyConfigId(topologyConfigId)
                    .setVersion(logReplicatorVersion)
                    .setLastLogEntryApplied(Address.NON_ADDRESS)
                    .setLastSnapshotTransferredSeqNumber(Address.NON_ADDRESS)
                    .setLastSnapshotApplied(Address.NON_ADDRESS)
                    .setLastSnapshotTransferred(Address.NON_ADDRESS)
                    .setLastSnapshotStarted(Address.NON_ADDRESS)
                    .setCurrentCycleMinShadowStreamTs(Address.NON_ADDRESS)
                    .setRemainingReplicationPercent(-1L)
                    .build();

            ReplicationStatus defaultSourceStatus = ReplicationStatus.newBuilder()
                    .setSourceStatus(SourceReplicationStatus.newBuilder()
                            .setRemainingEntriesToSend(-1L)
                            .setReplicationInfo(ReplicationInfo.newBuilder()
                                    .setStatus(SyncStatus.NOT_STARTED)
                                    .build())
                            .build())
                    .build();

            ReplicationStatus defaultSinkStatus = ReplicationStatus.newBuilder()
                    .setSinkStatus(SinkReplicationStatus.newBuilder()
                            .setDataConsistent(false)
                            .build())
                    .build();

            for (LogReplicationSession session : incomingSessions) {
                // Add an entry for this session if it does not exist, otherwise, this is a resuming/ongoing session
                if (!txn.keySet(metadataTable).contains(session)) {
                    log.debug("Adding entry for session={} in Replication Metadata Table", session);
                    txn.putRecord(metadataTable, session, defaultMetadata, null);
                }

                if (!txn.keySet(statusTable).contains(session)) {
                    log.debug("Adding entry for session={}[Sink] in Replication Status Table", session);
                    txn.putRecord(statusTable, session, defaultSinkStatus, null);
                }
            }

            for (LogReplicationSession session : outgoingSessions) {
                if (!txn.keySet(statusTable).contains(session)) {
                    log.debug("Adding entry for session={}[Source] in Replication Status Table", session);
                    txn.putRecord(statusTable, session, defaultSourceStatus, null);
                }
            }

            txn.commit();
        }
    }

    public TxnContext getTxnContext() {
        return corfuStore.txn(NAMESPACE);
    }

    // =========================== Replication Metadata Table Methods ===============================

    /**
     * Query the replication metadata / status for a given LR session
     *
     * @param session unique identifier for LR session
     * @return replication metadata info
     */
    public ReplicationMetadata queryReplicationMetadata(LogReplicationSession session) {
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
        CorfuStoreEntry<LogReplicationSession, ReplicationMetadata, Message> record = txn.getRecord(METADATA_TABLE_NAME, session);
        return record.getPayload();
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
        CorfuStoreEntry<LogReplicationSession, ReplicationMetadata, Message> record = txn.getRecord(metadataTable, session);
        ReplicationMetadata updatedMetadata = record.getPayload().toBuilder().setField(fd, value).build();
        txn.putRecord(metadataTable, session, updatedMetadata, null);

        log.trace("Update metadata field {}, value={}", fd.getFullName(), value);
    }

    /**
     * Update replication metadata for a given LR session.
     *
     * @param txn transaction context, for atomic commit
     * @param session unique identifier for LR Session
     * @param metadata new replication metadata object
     */
    public void updateReplicationMetadata(TxnContext txn, LogReplicationSession session, ReplicationMetadata metadata) {
        txn.putRecord(metadataTable, session, metadata, null);
    }

    /**
     * Add entries on metadata tables for given sessions
     *
     * */
    public void refresh(TopologyDescriptor topology) {
        processSessions(topology.getSessions());
        initializeMetadataTables(topology.getTopologyConfigId(), "");
    }

    // =========================== Replication Status Table Methods ===============================

    /**
     * Retrieve replication status for all sessions (incoming and outgoing)
     *
     */
    public Map<LogReplicationSession, ReplicationStatus> getReplicationStatus() {
        try(TxnContext txn = corfuStore.txn(NAMESPACE)) {
            Map<LogReplicationSession, ReplicationStatus> statusMap = new HashMap<>();
            List<CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message>> entries = txn.executeQuery(statusTable, e -> true);
            for (CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> entry : entries) {
                statusMap.put(entry.getKey(), entry.getPayload());
            }
            return statusMap;
        }
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
                            "persistedSnapshotStart={}",
                    topologyConfigId, snapshotStartTs, metadata.getTopologyConfigId(), metadata.getLastSnapshotStarted());
        }

        return (snapshotStartTs == metadata.getLastSnapshotStarted() && topologyConfigId == metadata.getTopologyConfigId());
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

            ReplicationMetadata metadata = queryReplicationMetadata(session);

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
            ReplicationStatus statusValue = ReplicationStatus.newBuilder()
                    .setSinkStatus(SinkReplicationStatus.newBuilder()
                            .setDataConsistent(true)
                            .setReplicationInfo(ReplicationInfo.newBuilder()
                                    .setStatus(SyncStatus.UNAVAILABLE)
                                    .build())
                            .build())
                    .build();
            txn.putRecord(statusTable, session, statusValue, null);
            txn.commit();

            log.debug("Commit snapshot apply complete timestamp={}, for topologyConfigId={}", ts, topologyConfigId);
        }
    }

    /**
     * Set the snapshot sync start marker, i.e., a unique identification of the current snapshot sync cycle.
     * Identified by the snapshot sync Id and the min shadow stream update timestamp for this cycle.
     *
     * @param session
     * @param newSnapshotCycleId
     * @param shadowStreamTs
     */
    public void setSnapshotSyncStartMarker(TxnContext txnContext, LogReplicationSession session, UUID newSnapshotCycleId,
                                           CorfuStoreMetadata.Timestamp shadowStreamTs) {
        // TODO: use passed TXN

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            ReplicationMetadata metadata = queryReplicationMetadata(txn, session);
            UUID currentSnapshotCycleId = new UUID(metadata.getCurrentSnapshotCycleId().getMsb(), metadata.getCurrentSnapshotCycleId().getLsb());

            // Update if current Snapshot Sync differs from the persisted one, otherwise ignore.
            // It could have already been updated in the case that leader changed in between a snapshot sync cycle
            if (currentSnapshotCycleId != newSnapshotCycleId) {
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
            txn.commit();
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
    public void addEvent(ReplicationEventKey key, ReplicationEvent event) {
        log.info("Add event :: {}", event);
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            txn.putRecord(replicationEventTable, key, event, null);
            txn.commit();
        }
    }

    /**
     * Subscribe to the logReplicationEventTable
     *
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

    // ================================= Replication Status Table Methods ===================================

    /**
     * Update replication status table's snapshot sync info as ongoing.
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
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
     * @param sinkClusterId standby cluster id
     */
    public void updateSnapshotSyncStatusCompleted(String sinkClusterId, long remainingEntriesToSend, long baseSnapshot) {
        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build();
        LogReplicationSession key = LogReplicationSession.newBuilder().setSinkClusterId(sinkClusterId).build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {

            CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> record = txn.getRecord(statusTable, key);

            if (record.getPayload() != null) {
                SourceReplicationStatus previous = record.getPayload().getSourceStatus();
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

                txn.putRecord(statusTable, key, current, null);
                txn.commit();

                snapshotSyncTimerSample
                        .flatMap(sample -> MeterRegistryProvider.getInstance()
                                .map(registry -> {
                                    Timer timer = registry.timer("logreplication.snapshot.duration");
                                    return sample.stop(timer);
                                }));

                log.debug("syncStatus :: set snapshot sync to COMPLETED and log entry ONGOING, clusterId: {}," +
                        " syncInfo: [{}]", sinkClusterId, currentSyncInfo);
            }
        }
    }

    /**
     * Update replication status table's sync status
     *
     * Note: TransactionAbortedException has been handled by upper level.
     *
     * @param clusterId sink cluster id
     */
    public void updateSyncStatus(String clusterId, SyncType lastSyncType, SyncStatus status) {
        LogReplicationSession key = LogReplicationSession.newBuilder().setSinkClusterId(clusterId).build();

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {

            CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> record = txn.getRecord(statusTable, key);

            // When a remote cluster has been removed from topology, the corresponding entry in the status table is
            // removed and FSM is shutdown. Since FSM shutdown is async, we ensure that we don't update a record which
            // has already been deleted.
            // (STOPPED status is used for other FSM states as well, so cannot rely only on the incoming status)
            if (record.getPayload() == null && status == SyncStatus.STOPPED) {
                log.debug("syncStatus :: ignoring update for {} to syncType {} and status {} as no record exists for the same",
                        clusterId, lastSyncType, status);
                return;
            }

            SourceReplicationStatus previous = record.getPayload() != null ? record.getPayload().getSourceStatus() : SourceReplicationStatus.newBuilder().build();
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

            txn.putRecord(statusTable, key, ReplicationStatus.newBuilder().setSourceStatus(current).build(), null);
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
     * @param clusterId sink cluster id
     * @param remainingEntries num of remaining entries to send
     * @param type sync type
     */
    public void setReplicationStatusTable(String clusterId, long remainingEntries, SyncType type) {
        LogReplicationSession key = LogReplicationSession.newBuilder().setSinkClusterId(clusterId).build();
        SnapshotSyncInfo snapshotStatus = null;
        ReplicationStatus current;
        ReplicationStatus previous = null;

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> record = txn.getRecord(statusTable, key);
            if (record.getPayload() != null) {
                previous = record.getPayload();
                snapshotStatus = previous.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo();
            }
            txn.commit();
        }

        if (type == SyncType.LOG_ENTRY) {
            if (previous != null &&
                    (previous.getSourceStatus().getReplicationInfo().getStatus().equals(SyncStatus.NOT_STARTED)
                            || snapshotStatus.getStatus().equals(SyncStatus.STOPPED))) {
                log.info("syncStatusPoller :: skip replication status update, log entry replication is {}",
                        previous.getSourceStatus().getReplicationInfo().getStatus());
                // Skip update of sync status, it will be updated once replication is resumed or started
                return;
            }

            if (snapshotStatus == null){
                log.warn("syncStatusPoller [logEntry]:: previous snapshot status is not present for cluster: {}", clusterId);
                snapshotStatus = SnapshotSyncInfo.newBuilder().build();
            }

            current = ReplicationStatus.newBuilder()
                    .setSourceStatus(SourceReplicationStatus.newBuilder()
                            .setRemainingEntriesToSend(remainingEntries)
                            .setReplicationInfo(ReplicationInfo.newBuilder()
                                    .setSyncType(type)
                                    .setStatus(SyncStatus.ONGOING)
                                    .setSnapshotSyncInfo(snapshotStatus)
                                    .build())
                            .build())
                    .build();

            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(statusTable, key, current, null);
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

            current = ReplicationStatus.newBuilder()
                    .setSourceStatus(SourceReplicationStatus.newBuilder()
                            .setRemainingEntriesToSend(remainingEntries)
                            .setReplicationInfo(ReplicationInfo.newBuilder()
                                    .setSyncType(type)
                                    .setStatus(SyncStatus.ONGOING)
                                    .setSnapshotSyncInfo(currentSnapshotSyncInfo)
                                    .build())
                            .build())
                    .build();

            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(statusTable, key, current, null);
                txn.commit();
            }

            log.debug("syncStatusPoller :: sync status for {} set to ONGOING, clusterId: {}, remainingEntries: {}",
                    type, clusterId, remainingEntries);
        }
    }

    /**
     *
     * @return
     */
    public Map<LogReplicationSession, ReplicationStatus> getReplicationRemainingEntries() {
        Map<LogReplicationSession, ReplicationStatus> replicationStatusMap = new HashMap<>();
        List<CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message>> entries;
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            entries = txn.executeQuery(statusTable, record -> true);
            txn.commit();
        }

        for (CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> entry : entries) {
            LogReplicationSession session = entry.getKey();
            ReplicationStatus status = entry.getPayload();
            replicationStatusMap.put(session, status);
            log.debug("getReplicationRemainingEntries: session={}, remainingEntriesToSend={}, " +
                            "syncType={}", session, status.getSourceStatus().getRemainingEntriesToSend(),
                    status.getSourceStatus().getReplicationInfo().getSyncType());
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
     * @param session log replication session identifier
     */
    public void setDataConsistentOnStandby(boolean isConsistent, LogReplicationSession session) {
        SinkReplicationStatus status = SinkReplicationStatus.newBuilder()
                .setDataConsistent(isConsistent)
                .build();
        try (TxnContext txn = getTxnContext()) {
            txn.putRecord(statusTable, session, ReplicationStatus.newBuilder().setSinkStatus(status).build(), null);
            txn.commit();
        }

        log.debug("setDataConsistentOnStandby: localClusterId: {}, isConsistent: {}", session.getSinkClusterId(), isConsistent);
    }

    public Map<LogReplicationSession, SinkReplicationStatus> getDataConsistentOnStandby(LogReplicationSession session) {
        CorfuStoreEntry<LogReplicationSession, ReplicationStatus, Message> record;
        SinkReplicationStatus status;

        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            record = txn.getRecord(statusTable, session);
            txn.commit();
        }

        // Initially, snapshot sync is pending so the data is not consistent.
        if (record.getPayload() == null) {
            log.warn("DataConsistent status is not set for session {}", session);
            status = SinkReplicationStatus.newBuilder().setDataConsistent(false).build();
        } else {
            status = record.getPayload().getSinkStatus();
        }
        Map<LogReplicationSession, SinkReplicationStatus> dataConsistentMap = new HashMap<>();
        dataConsistentMap.put(session, status);

        log.debug("getDataConsistentOnStandby: session: {}, status: {}", session, status);

        return dataConsistentMap;
    }

    /**
     * Reset replication status for all sessions
     */
    public void resetReplicationStatus() {
        log.info("Reset replication status for all LR sessions");
        try (TxnContext tx = corfuStore.txn(NAMESPACE)) {
            tx.clear(statusTable);
            tx.commit();
        }
    }

    // ================================ Runtime Helper Functions ======================================

    public long getLogHead() {
        return runtime.getAddressSpaceView().getTrimMark().getSequence();
    }

    // ================================ End Runtime Helper Functions ==================================

    public void removeFromStatusTable(LogReplicationSession session) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                    if (txn.isExists(statusTable, session)) {
                        txn.delete(statusTable, session);
                        txn.commit();
                    } else {
                        log.warn("Status for session {} does not exist.", session);
                    }
                } catch(TransactionAbortedException e) {
                    log.error("Exception while removing status from table", e);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when updating the topology " +
                    "config id", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
        log.debug("Successfully deleted session {} from {}", session, REPLICATION_STATUS_TABLE_NAME_SOURCE);
    }

    /**
     * Reset manager by clearing all tables
     */
    public void reset() {
        log.info("Reset all metadata manager tables");
        try (TxnContext tx = corfuStore.txn(NAMESPACE)) {
            statusTable.clearAll();
            metadataTable.clearAll();
            replicationEventTable.clearAll();
            tx.commit();
        }
    }

    public void removeSession(LogReplicationSession session) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                    txn.delete(statusTable, session);
                    txn.delete(metadataTable, session);
                    txn.commit();
                } catch (TransactionAbortedException e) {

                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception while removing session", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
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
        DATA_CONSISTENT_ON_STANDBY("dataConsistentOnStandby"),
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
