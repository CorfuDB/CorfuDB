package org.corfudb.infrastructure.logreplication.utils;

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEvent.ReplicationEventType;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationEventInfoKey;
import org.corfudb.infrastructure.logreplication.replication.receive.ReplicationWriterException;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Instant;
import java.util.UUID;

import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.NAMESPACE;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_EVENT_TABLE_NAME;


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
    public static void enforceSnapshotSync(LogReplicationSession session, CorfuStore corfuStore,
                                           ReplicationEventType eventType) {
        UUID forceSyncId = UUID.randomUUID();

        log.info("Forced snapshot sync will be triggered, session={}, sync_id={}",
                session, forceSyncId);

        // Write a force sync event to the logReplicationEventTable
        ReplicationEventInfoKey key = ReplicationEventInfoKey.newBuilder()
                .setSession(session)
                .build();

        ReplicationEvent event = ReplicationEvent.newBuilder()
                .setEventId(forceSyncId.toString())
                .setType(eventType)
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
}
