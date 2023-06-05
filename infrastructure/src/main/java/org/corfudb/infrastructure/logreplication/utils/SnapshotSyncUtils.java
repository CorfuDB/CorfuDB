package org.corfudb.infrastructure.logreplication.utils;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent.ReplicationEventType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventInfoKey;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.receive.ReplicationWriterException;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.NAMESPACE;
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
            try {
                replicationEventTable = corfuStore.openTable(NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                        ReplicationEventInfoKey.class,
                        ReplicationEvent.class,
                        null,
                        TableOptions.fromProtoSchema(ReplicationEvent.class));
            } catch (Exception e) {
                log.error("Caught an exception while opening LogReplicationEventTable", e);
                throw new ReplicationWriterException(e);
            }
        }

        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
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

    public static void removeUpgradeSnapshotSyncEvent(LogReplicationEvent event, CorfuStore corfuStore) {
        Table<LogReplicationMetadata.ReplicationEventInfoKey, LogReplicationMetadata.ReplicationEvent, Message> replicationEventTable =
                tryOpenTable(corfuStore, CORFU_SYSTEM_NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                        ReplicationEventInfoKey.class, ReplicationEvent.class, null);

        List<CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message>> events;
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            events = txn.executeQuery(REPLICATION_EVENT_TABLE_NAME, p -> true);
            txn.commit();
        } catch (Exception e) {
            log.error("Failed to get the replication events", e);
            return;
        }

        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            boolean entryFound = false;
            for (CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message> entry : events) {
                if (entry.getPayload().getEventId().equals(String.valueOf(event.getMetadata().getRequestId()))) {
                    txnContext.delete(replicationEventTable, entry.getKey());
                    entryFound = true;
                    break;
                }
            }

            if (entryFound) {
                log.info("Upgrade forced sync event removed, request id: {}", event.getMetadata().getRequestId());
            } else {
                log.info("No force sync event found with matching request id: {}", event.getMetadata().getRequestId());
            }
            txnContext.commit();
        } catch (TransactionAbortedException e) {
            log.error("TX Abort while trying to remove force sync event.", e);
        }
    }

    // TODO (Shreay): Verify all possible paths for force sync re-triggers
    public static void modifyUpgradeSnapshotSyncEvent(UUID oldEventId, UUID newEventID, CorfuStore corfuStore) {
        Table<LogReplicationMetadata.ReplicationEventInfoKey, LogReplicationMetadata.ReplicationEvent, Message> replicationEventTable =
                tryOpenTable(corfuStore, CORFU_SYSTEM_NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                        ReplicationEventInfoKey.class, ReplicationEvent.class, null);

        List<CorfuStoreEntry<ReplicationEventInfoKey, ReplicationEvent, Message>> events;
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            events = txn.executeQuery(REPLICATION_EVENT_TABLE_NAME, p -> true);
            txn.commit();
        } catch (Exception e) {
            log.error("Failed to get the replication events", e);
            return;
        }

        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            boolean entryFound = false;
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

            if (entryFound) {
                log.info("Upgrade forced sync event modified, old event ID: {}, new event ID: {}", oldEventId, newEventID);
            } else {
                log.info("No force sync event found with matching event ID: {}", oldEventId);
            }
            txnContext.commit();
        } catch (TransactionAbortedException e) {
            log.error("TX Abort while trying to modify force sync event.", e);
        }
    }
}
