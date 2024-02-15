package org.corfudb.infrastructure.logreplication.utils;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent.ReplicationEventType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventInfoKey;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.tryOpenTable;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;


/**
 * TODO (V2): reason enum to differentiate the paths that trigger a forced snapshot sync
 * This class provides utility method for enforcing snapshot sync. For example, for LOGICAL_GROUP model, if a new
 * group is added to a Sink destination that has an on-going logical group replication session, a forced snapshot
 * sync should be triggered for the session to keep the Sink consistent with Source side.
 */
@Slf4j
public final class SnapshotSyncUtils {

    private static Table<ReplicationEventInfoKey, ReplicationEvent, Message> replicationEventTable;

    private SnapshotSyncUtils() {
        // prevent instantiation
    }

    /**
     * Enforce snapshot sync by writing a force sync event to the logReplicationEventTable.
     *
     * @param session Session for which the snapshot sync will be enforced.
     * @param corfuStore Caller of this method should provide a corfuStore instance for executing the transaction.
     */
    public static void enforceSnapshotSync(LogReplicationSession session, CorfuStore corfuStore) {
        UUID forceSyncId = UUID.randomUUID();

        log.info("Forced snapshot sync will be triggered because of group destination change, session={}, sync_id={}",
                session, forceSyncId);

        // Write a force sync event to the logReplicationEventTable
        ReplicationEventInfoKey key = ReplicationEventInfoKey.newBuilder()
                .setSession(session)
                .build();

        ReplicationEvent event = ReplicationEvent.newBuilder()
                .setEventId(forceSyncId.toString())
                .setType(ReplicationEventType.FORCE_SNAPSHOT_SYNC)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .build();

        if (replicationEventTable == null) {
            replicationEventTable = tryOpenTable(corfuStore, CORFU_SYSTEM_NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                    ReplicationEventInfoKey.class, ReplicationEvent.class, null);
        }

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    txn.putRecord(replicationEventTable, key, event, null);
                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.warn("TXAbort while adding enforce snapshot sync event, retrying", tae);
                    throw new RetryNeededException();
                }
                log.debug("Added enforce snapshot sync event to the logReplicationEventTable for session={}", session);
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception while adding enforce snapshot sync event", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    /**
     * Enforce snapshot sync for upgrade by writing an UPGRADE_COMPLETION_FORCE_SNAPSHOT_SYNC event
     * to the logReplicationEventTable.
     *
     * @param session Session for which the snapshot sync will be enforced.
     * @param txnContext Caller of this method should provide the txnContext and handle exceptions.
     */
    public static void addUpgradeSnapshotSyncEvent(LogReplicationSession session, TxnContext txnContext) {
        UUID forceSyncId = UUID.randomUUID();

        // Write a rolling upgrade force snapshot sync event to the logReplicationEventTable
        LogReplicationMetadata.ReplicationEventInfoKey key = LogReplicationMetadata.ReplicationEventInfoKey.newBuilder()
                .setSession(session)
                .build();

        LogReplicationMetadata.ReplicationEvent event = LogReplicationMetadata.ReplicationEvent.newBuilder()
                .setEventId(forceSyncId.toString())
                .setType(LogReplicationMetadata.ReplicationEvent.ReplicationEventType.UPGRADE_COMPLETION_FORCE_SNAPSHOT_SYNC)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .build();

        replicationEventTable = txnContext.getTable(REPLICATION_EVENT_TABLE_NAME);

        log.info("Forced snapshot sync will be triggered due to completion of rolling upgrade for event with id: {}", forceSyncId);
        txnContext.putRecord(replicationEventTable, key, event, null);
    }

    public static void removeUpgradeSnapshotSyncEvent(UUID eventId, CorfuStore corfuStore, ExecutorService executor) {
        replicationEventTable = tryOpenTable(corfuStore, CORFU_SYSTEM_NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                ReplicationEventInfoKey.class, ReplicationEvent.class, null);

        List<CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message>> events =
                retrievalReplicationEvents(corfuStore);

        try {
            IRetry.build(IntervalRetry.class, () -> {
                boolean entryFound = false;
                try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    for (CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message> entry : events) {
                        if (entry.getPayload().getEventId().equals(String.valueOf(eventId))) {
                            txnContext.delete(replicationEventTable, entry.getKey());
                            entryFound = true;
                            break;
                        }
                    }
                    txnContext.commit();
                } catch (TransactionAbortedException tae) {
                    log.warn("TX Abort while trying to remove force sync event.", tae);
                    throw new RetryNeededException();
                }
                if (entryFound) {
                    log.info("Upgrade forced sync event removed, request id: {}", eventId);
                } else {
                    log.info("No force sync event found with matching request id: {}", eventId);
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception while removing enforce snapshot sync event", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        } finally {
            executor.shutdown();
        }
    }

    public static void modifyUpgradeSnapshotSyncEvent(UUID oldEventId, UUID newEventID, CorfuStore corfuStore) {
        replicationEventTable = tryOpenTable(corfuStore, CORFU_SYSTEM_NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                ReplicationEventInfoKey.class, ReplicationEvent.class, null);

        List<CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message>> events =
                retrievalReplicationEvents(corfuStore);

        try {
            IRetry.build(IntervalRetry.class, () -> {
                boolean entryFound = false;
                try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    for (CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message> entry : events) {
                        if (entry.getPayload().getEventId().equals(oldEventId.toString())) {
                            // Modify the existing entry with the new event ID
                            LogReplicationMetadata.ReplicationEvent modifiedEvent = entry.getPayload().toBuilder()
                                    .setType(ReplicationEventType.UPGRADE_COMPLETION_FORCE_SNAPSHOT_SYNC_RETRIGGER)
                                    .setEventId(newEventID.toString())
                                    .build();
                            txnContext.putRecord(replicationEventTable, entry.getKey(), modifiedEvent, entry.getMetadata());
                            entryFound = true;
                            break;
                        }
                    }
                    txnContext.commit();
                } catch (TransactionAbortedException tae) {
                    log.error("TX Abort while trying to modify force sync event.", tae);
                }
                if (entryFound) {
                    log.info("Upgrade force sync event modified, old event ID: {}, new event ID: {}", oldEventId, newEventID);
                } else {
                    log.info("No force sync event found with matching event ID: {}", oldEventId);
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception while modifying enforce snapshot sync event", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    private static List<CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message>> retrievalReplicationEvents(CorfuStore corfuStore) {
        List<CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message>> events = new ArrayList<>();
        try {
            IRetry.build(IntervalRetry.class, () -> {
                List<CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message>> queriedEvents;
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    queriedEvents = new ArrayList<>(txn.executeQuery(REPLICATION_EVENT_TABLE_NAME, p -> true));
                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.warn("TXAbort while retrieving the replication events, retrying", tae);
                    throw new RetryNeededException();
                }
                events.addAll(queriedEvents);
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception while retrieving the replication events", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
        return events;
    }
}
