package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationEvent;
import org.corfudb.runtime.LogReplication.ReplicationEventInfoKey;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.LogReplication.ReplicationSubscriber;
import org.corfudb.runtime.Queue.CorfuGuidMsg;
import org.corfudb.runtime.Queue.CorfuQueueMetadataMsg;
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
import org.corfudb.runtime.Queue.RoutingQSnapSyncHeaderMsg;
import org.corfudb.runtime.Queue.RoutingQSnapSyncHeaderKeyMsg;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.ScopedTransaction;
import org.corfudb.runtime.collections.StreamListenerResumeOrDefault;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.LogReplicationClientException;
import org.corfudb.runtime.view.TableRegistry;

import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.util.serializer.ProtobufSerializer;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.SNAP_SYNC_TXN_ENVELOPE_TABLE;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.lrSnapSyncTxnEnvelopeStreamId;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class RoutingQueueSenderClient extends LogReplicationClient implements LogReplicationRoutingQueueClient {

    private final CorfuStore corfuStore;

    private final String clientName;

    // TODO (V2): This field should be removed after the rpc stream is added for Sink side session creation.
    public static final String DEFAULT_ROUTING_QUEUE_CLIENT = "00000000-0000-0000-0000-0000000000002";


    // TODO: Find a way to use these from a common location (they are in infrastructure currently)
    private static final String REPLICATION_EVENT_TABLE_NAME = "LogReplicationEventTable";
    private static final String LR_STREAM_TAG = "log_replication";

    private Table<CorfuGuidMsg, RoutingTableEntryMsg, CorfuQueueMetadataMsg> logEntryQ;
    private Table<CorfuGuidMsg, RoutingTableEntryMsg, CorfuQueueMetadataMsg> snapSyncQ;
    private Table<ReplicationEventInfoKey, ReplicationEvent, Message> replicationEventTable;
    private SnapSyncRequestor fullSyncRequestor;

    /**
     * Constructor for the log replication client for routing queues on sender.
     *
     * @param corfuStore CorfuStore instance used by the jvm
     * @param clientName String representation of the client name. This parameter is case-sensitive.
     * @throws IllegalArgumentException If clientName is null or empty.
     * @throws NoSuchMethodException    NoSuchMethodException.
     * @throws IllegalAccessException   IllegalAccessException.
     */
    public RoutingQueueSenderClient(CorfuStore corfuStore, String clientName) throws Exception {
        Preconditions.checkArgument(isValid(clientName), "clientName is null or empty.");

        this.corfuStore = corfuStore;
        this.clientName = clientName;

        Table<CorfuGuidMsg, RoutingTableEntryMsg, CorfuQueueMetadataMsg> logEntryQLocal;
        Table<CorfuGuidMsg, RoutingTableEntryMsg, CorfuQueueMetadataMsg> snapSyncQLocal;

        try {
            logEntryQLocal = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, LOG_ENTRY_SYNC_QUEUE_NAME_SENDER,
                RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(RoutingTableEntryMsg.class));
            snapSyncQLocal = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, SNAPSHOT_SYNC_QUEUE_NAME_SENDER,
                RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(RoutingTableEntryMsg.class));
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, SNAP_SYNC_TXN_ENVELOPE_TABLE,
                RoutingQSnapSyncHeaderKeyMsg.class, RoutingQSnapSyncHeaderMsg.class, null,
                TableOptions.fromProtoSchema(RoutingTableEntryMsg.class));
            replicationEventTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                ReplicationEventInfoKey.class,
                ReplicationEvent.class,
                null,
                TableOptions.fromProtoSchema(ReplicationEvent.class));
        } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
            log.error("Failed to open table/queue", e);
            throw new LogReplicationClientException(e);
        }

        this.logEntryQ = logEntryQLocal;
        this.snapSyncQ = snapSyncQLocal;
        register(corfuStore, clientName, ReplicationModel.ROUTING_QUEUES);
    }

    public void startLRSnapshotTransmitter(LRTransmitterReplicationModule snapSyncProvider) {
        this.fullSyncRequestor = new SnapSyncRequestor(
                corfuStore, snapSyncProvider, CORFU_SYSTEM_NAMESPACE,
                LR_STREAM_TAG, Collections.singletonList(REPLICATION_EVENT_TABLE_NAME));

        corfuStore.subscribeListener(fullSyncRequestor, CORFU_SYSTEM_NAMESPACE,
                LR_STREAM_TAG, Collections.singletonList(REPLICATION_EVENT_TABLE_NAME));
    }

    public void stopLRSnapshotTransmitter() {
        corfuStore.unsubscribeListener(fullSyncRequestor);
    }

    public interface LRTransmitterReplicationModule {
        /**
         * Full state data is requested for the application.
         * It is expected that this call is non blocking and data will be provided in different thread.
         *
         * @param context replication context
         */
        void provideFullStateData(LRFullStateReplicationContext context);

        void cancel(LRFullStateReplicationContext context);
    }

    private class SnapSyncRequestor extends StreamListenerResumeOrDefault {

        private final LRTransmitterReplicationModule snapSyncProvider;
        private final ConcurrentHashMap<String, SnapshotSyncDataTransmitter> pendingFullSyncsPerDestination;

        public SnapSyncRequestor(CorfuStore store, LRTransmitterReplicationModule snapSyncProvider,
                                 String namespace, String streamTag, List<String> tablesOfInterest) {
            super(store, namespace, streamTag, tablesOfInterest);
            this.snapSyncProvider = snapSyncProvider;
            this.pendingFullSyncsPerDestination = new ConcurrentHashMap<>();
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            // TODO: Check for existing events. What if subscription comes later than the event is published.
            log.info("onNext[{}] :: got updates on RoutingQSender for tables {}", results.getTimestamp(),
                    results.getEntries().keySet().stream().map(TableSchema::getTableName).collect(Collectors.toList()));
            ReplicationEvent fullSyncEvent = null;
            ReplicationEventInfoKey key = null;
            // Any notification here indicates a snapshot sync request
            for (List<CorfuStreamEntry> entryList : results.getEntries().values()) {
                for (CorfuStreamEntry entry : entryList) {
                    if (entry.getOperation() == CorfuStreamEntry.OperationType.CLEAR ||
                            entry.getOperation() == CorfuStreamEntry.OperationType.DELETE) {
                        log.warn("RoutingQEventListener ignoring a {} operation", entry.getOperation());
                        continue;
                    }
                    key = (ReplicationEventInfoKey) entry.getKey();
                    fullSyncEvent = (ReplicationEvent) entry.getPayload();
                    if (fullSyncEvent.getType() !=
                            ReplicationEvent.ReplicationEventType.SERVER_REQUESTED_SNAPSHOT_SYNC
                            || !clientName.equals(key.getSession().getSubscriber().getClientName())) {
                        fullSyncEvent = null;
                        continue;
                    }
                    log.info("Full Sync requested due to event :: id={}, type={}, session={}, ts={}",
                            fullSyncEvent.getEventId(), fullSyncEvent.getType(),
                            key.getSession(), fullSyncEvent.getEventTimestamp());
                }
            }
            if (fullSyncEvent != null) {
                SnapshotSyncDataTransmitter snapshotSyncDataTransmitter = new SnapshotSyncDataTransmitter(
                        fullSyncEvent, key);

                SnapshotSyncDataTransmitter previousRequestForSameDestination =
                        pendingFullSyncsPerDestination.get(key.getSession().getSinkClusterId());
                if (previousRequestForSameDestination != null) {
                    log.info("Cancelling prior full sync request for destination {} ",
                            snapshotSyncDataTransmitter.getFullSyncRequestIdFromLR());
                    snapSyncProvider.cancel(previousRequestForSameDestination);
                    pendingFullSyncsPerDestination.remove(key.getSession().getSinkClusterId());
                    previousRequestForSameDestination.cancel();
                }
                pendingFullSyncsPerDestination.put(key.getSession().getSinkClusterId(), snapshotSyncDataTransmitter);
                snapSyncProvider.provideFullStateData(snapshotSyncDataTransmitter);
            }
        }

        Map<String, LRFullStateReplicationContext> getPendingFullSyncs() {
            return new HashMap<>(pendingFullSyncsPerDestination);
        }

        /**
         * This class contains methods that are invoked by the RoutingQueueSender's full sync provider to transmit
         * batches of full sync data via Log Replicator.
         */
        private class SnapshotSyncDataTransmitter implements LRFullStateReplicationContext {

            @Getter
            @Setter
            ScopedTransaction snapshot = null;

            private long baseSnapshotTimestamp;

            private final ReplicationEvent requestingEvent;

            ReplicationEventInfoKey key;

            // destination cluster Id + log tail at time of LR Server's full sync request
            // this forms the key to distinguish different full sync data requests from routing queue client.
            @Getter
            RoutingQSnapSyncHeaderKeyMsg fullSyncRequestIdFromLR;

            public SnapshotSyncDataTransmitter(ReplicationEvent requestingEvent,
                                               ReplicationEventInfoKey key) {
                this.requestingEvent = requestingEvent;
                this.baseSnapshotTimestamp = 0L;
                this.fullSyncRequestIdFromLR = RoutingQSnapSyncHeaderKeyMsg.newBuilder()
                        .setSnapSyncRequestId(requestingEvent.getEventTimestamp().getSeconds())
                        .setDestination(key.getSession().getSinkClusterId()).build();
                this.key = key;
            }

            @Override
            public TxnContext getTxn() {
                if (TransactionalContext.isInTransaction()) {
                    return TransactionalContext.getRootContext().getTxnContext();
                }
                return null;
            }

            @Override
            public String getDestinationSiteId() {
                return key.getSession().getSinkClusterId();
            }

            @Override
            public UUID getRequestId() {
                return UUID.fromString(requestingEvent.getEventId());
            }

            @Nullable
            @Override
            public ReplicationEvent.ReplicationEventType getReason() {
                return requestingEvent.getType();
            }

            @Override
            public void transmit(RoutingTableEntryMsg message) throws CancellationException {
                transmit(message, 0);
            }

            @Override
            public void transmit(RoutingTableEntryMsg message, int progress) throws CancellationException {
                log.trace("Enqueuing message to snapshot sync queue, message: {}", message);
                getTxn().logUpdateEnqueue(snapSyncQ.getStreamUUID(), message, message.getDestinationsList().stream()
                        .map(destination -> TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                                SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + destination + "_" + clientName))
                        .collect(Collectors.toList()), corfuStore);

                // Only once per transaction we set up the header to identify the request from LR server
                if (TransactionalContext.getRootContext().getWriteSetInfo().getWriteSet().getSMRUpdates(lrSnapSyncTxnEnvelopeStreamId).isEmpty()) {
                    if (baseSnapshotTimestamp == 0) {
                        if (snapshot != null) {
                            baseSnapshotTimestamp = snapshot.getTxnSnapshot().getSequence();
                            log.info("SnapSync base snapshot from Scoped Transaction = {}", baseSnapshotTimestamp);
                        } else {
                            baseSnapshotTimestamp = getTxn().getTxnSequence();
                            log.info("SnapSync base snapshot from first snapshot sync transaction = {}", baseSnapshotTimestamp);
                        }
                    }
                    RoutingQSnapSyncHeaderMsg startMarker = RoutingQSnapSyncHeaderMsg.newBuilder()
                            .setSnapshotStartTimestamp(baseSnapshotTimestamp) // This is the base snapshot timestamp
                            .build();

                    CorfuRecord<RoutingQSnapSyncHeaderMsg, Message> markerEntry =
                            new CorfuRecord<>(startMarker, null);

                    Object[] smrArgs = new Object[2];
                    smrArgs[0] = fullSyncRequestIdFromLR;
                    smrArgs[1] = markerEntry;
                    getTxn().logUpdate(lrSnapSyncTxnEnvelopeStreamId,
                            new SMREntry("put", smrArgs,
                                    corfuStore.getRuntime().getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE)),
                            message.getDestinationsList().stream()
                                    .map(destination -> TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                                            SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + destination + "_" + clientName))
                                    .collect(Collectors.toList())
                    );
                }
            }

            @Override
            public void markCompleted() throws CancellationException {
                log.info("Got completion marker sinkId={} requestID={}", key.getSession().getSinkClusterId(), requestingEvent.getEventTimestamp().getSeconds());
                RoutingQSnapSyncHeaderMsg startMarker = RoutingQSnapSyncHeaderMsg.newBuilder()
                        .setSnapshotStartTimestamp(-requestingEvent.getEventTimestamp().getSeconds())
                        .build();

                CorfuRecord<RoutingQSnapSyncHeaderMsg, Message> markerEntry =
                        new CorfuRecord<>(startMarker, null);

                Object[] smrArgs = new Object[2];
                smrArgs[0] = fullSyncRequestIdFromLR;
                smrArgs[1] = markerEntry;

                try {
                    IRetry.build(IntervalRetry.class, () -> {
                        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                            txnContext.logUpdate(lrSnapSyncTxnEnvelopeStreamId, new SMREntry("put", smrArgs,
                                    corfuStore.getRuntime().getSerializers().getSerializer(
                                        ProtobufSerializer.PROTOBUF_SERIALIZER_CODE)),
                                    Arrays.asList(TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                                            SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX +
                                                    key.getSession().getSinkClusterId() + "_" + clientName))
                            );
                            txnContext.commit();
                        } catch (TransactionAbortedException tae) {
                            log.error("Error while attempting to insert an END_MARKER", tae);
                            throw new RetryNeededException();
                        }
                        return null;
                    }).run();
                } catch (InterruptedException e) {
                    log.error("Unrecoverable exception when attempting to insert an END_MARKER", e);
                    throw new UnrecoverableCorfuInterruptedError(e);
                }

                if (getSnapshot() != null) {
                    getSnapshot().close();
                    setSnapshot(null);
                }
                pendingFullSyncsPerDestination.remove(key.getSession().getSinkClusterId());
            }

            @Override
            public void cancel() {
                // TODO: Need to figure out what might be LR's equivalent of a snapshot sync cancellation?
                if (getSnapshot() != null) {
                    getSnapshot().close();
                    setSnapshot(null);
                }
                pendingFullSyncsPerDestination.remove(key.getSession().getSinkClusterId());
            }
        }
    }

    /**
     * Enqueues message to be replicated onto the sender's delta queue.
     * Caller must set the replication type to LOG_ENTRY_SYNC
     *
     * @param txn     Transaction context in which the operation will be performed
     * @param message RoutingTableEntryMsg
     */
    @Override
    public void transmitDeltaMessage(TxnContext txn, RoutingTableEntryMsg message, CorfuStore corfuStore) throws Exception {
        log.trace("Enqueuing message to delta queue, message: {}", message);
        txn.logUpdateEnqueue(logEntryQ.getStreamUUID(), message, message.getDestinationsList().stream()
                .map(destination -> {
                    UUID streamIdForTag = TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                            LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + destination + "_" + clientName);
                    log.info("Adding destination stream tag id = {}", streamIdForTag);
                    return streamIdForTag;
                })
                .collect(Collectors.toList()), corfuStore);
    }

    /**
     * Enqueues messages to be replicated onto the sender's delta queue.
     *
     * @param txn      Transaction context in which the operation will be performed
     * @param messages List of RoutingTableEntryMsg
     */
    @Override
    public void transmitDeltaMessages(TxnContext txn, List<RoutingTableEntryMsg> messages, CorfuStore corfuStore) throws Exception {
        for (RoutingTableEntryMsg message : messages) {
            transmitDeltaMessage(txn, message, corfuStore);
        }
    }

    /**
     * Request LR to perform a forced snapshot sync, re-use parent transaction.
     *
     * @param txn - parent transaction if this request belongs in one.
     * @param sourceClusterId Id of the Source Cluster
     * @param sinkClusterId   Id of the Sink Cluster
     */
    public void requestSnapshotSync(TxnContext txn, String sourceClusterId, String sinkClusterId) {
        requestSnapshotSync(txn, sourceClusterId, sinkClusterId,
                ReplicationEvent.ReplicationEventType.CLIENT_REQUESTED_FORCED_SNAPSHOT_SYNC);
    }

    /**
     * Request LR to perform a forced snapshot sync for a specific remote site used outside a transaction.
     *
     * @param sourceClusterId Id of the Source Cluster
     * @param sinkClusterId   Id of the Sink Cluster
     */
    public void requestSnapshotSync(String sourceClusterId, String sinkClusterId) {
        try {
            IRetry.build(IntervalRetry.class, () -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    requestSnapshotSync(txn, sourceClusterId, sinkClusterId,
                            ReplicationEvent.ReplicationEventType.CLIENT_REQUESTED_FORCED_SNAPSHOT_SYNC);
                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.warn("TXAbort while requesting snapshot sync from {} to {}, retrying",
                            sourceClusterId, sinkClusterId, tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError("requestGlobalSnapshotSync Runtime Exception", e);
        }
    }

    /**
     * Request LR to perform a forced snapshot sync.
     *
     * @param txn - parent transaction if this request belongs in one.
     * @param sourceClusterId Id of the Source Cluster
     * @param sinkClusterId   Id of the Sink Cluster
     * @param replicationEventType request one or all
     */
    public ReplicationEventInfoKey requestSnapshotSync(TxnContext txn, String sourceClusterId, String sinkClusterId,
                                                       ReplicationEvent.ReplicationEventType replicationEventType) {
        ReplicationSubscriber subscriber = ReplicationSubscriber.newBuilder()
            .setClientName(this.clientName)
            .setModel(ReplicationModel.ROUTING_QUEUES)
            .build();
        LogReplicationSession session =
            LogReplicationSession.newBuilder()
                .setSourceClusterId(sourceClusterId)
                .setSinkClusterId(sinkClusterId)
                .setSubscriber(subscriber).build();
        return enforceSnapshotSync(txn, session, replicationEventType);
    }

    /**
     * Request LR to perform a forced snapshot sync for ALL remote sites (To be used for testing only)
     *
     * @param sourceClusterId Id of the Source Cluster
     * @param sinkClusterId   Id of the Sink Cluster
     */
    public void requestGlobalSnapshotSync(String sourceClusterId, String sinkClusterId) {
        log.warn("Requesting a snapshot sync for ALL remote sites at once!");
        try {
            IRetry.build(IntervalRetry.class, () -> {
                ReplicationEventInfoKey keyToDelete = null;
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    keyToDelete = requestSnapshotSync(txn, sourceClusterId, sinkClusterId,
                            ReplicationEvent.ReplicationEventType.UPGRADE_COMPLETION_FORCE_SNAPSHOT_SYNC);
                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.warn("TXAbort while adding global force snapshot sync event, retrying", tae);
                    throw new RetryNeededException();
                }

                // Immediately delete this event since we are not doing an upgrade, just re-using that mechanism
                // to trigger a global snapshot sync. Without deleting the event, LR server upon restarts will assume
                // that snapshot sync needs to be triggered once again and keep triggering snapshot syncs even when
                // other new sites are on-boarded.
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    txn.delete(replicationEventTable, keyToDelete);
                    txn.commit();
                } catch (TransactionAbortedException tae) {
                    log.warn("TXAbort while removing the global force snapshot sync event, retrying", tae);
                    throw new RetryNeededException();
                }
                return null;
            }).run();
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError("requestGlobalSnapshotSync Runtime Exception", e);
        }
    }

    public Map<String, LRFullStateReplicationContext> getPendingFullSyncs() {
        return fullSyncRequestor.getPendingFullSyncs();
    }

    // TODO pankti:  Move SnapshotSyncUtils to the 'runtime' package so that this method can be reused from there
    private ReplicationEventInfoKey enforceSnapshotSync(TxnContext txn, LogReplicationSession session,
                                     ReplicationEvent.ReplicationEventType eventType) {
        UUID forceSyncId = UUID.randomUUID();

        log.info("Forced snapshot sync will be triggered, session={}, sync_id={}", session, forceSyncId);

        // Write a force sync event to the logReplicationEventTable
        ReplicationEventInfoKey key = ReplicationEventInfoKey.newBuilder()
                .setSession(session)
                .build();

        ReplicationEvent event = ReplicationEvent.newBuilder()
                .setEventId(forceSyncId.toString())
                .setType(eventType)
                .setEventTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .build();
        txn.putRecord(replicationEventTable, key, event, null);
        return key;
    }
}
