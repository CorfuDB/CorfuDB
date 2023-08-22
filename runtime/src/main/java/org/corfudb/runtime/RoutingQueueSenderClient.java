package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
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
import org.corfudb.runtime.view.TableRegistry;

import org.corfudb.runtime.exceptions.StreamingException;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

import static org.corfudb.runtime.LogReplicationUtils.*;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class RoutingQueueSenderClient extends LogReplicationClient implements LogReplicationRoutingQueueClient {
    private static final ReplicationModel model = ReplicationModel.ROUTING_QUEUES;

    private final CorfuStore corfuStore;

    private final String clientName;

    // TODO (V2): This field should be removed after the rpc stream is added for Sink side session creation.
    public static final String DEFAULT_ROUTING_QUEUE_CLIENT = "00000000-0000-0000-0000-0000000000002";

    // TODO: Find a way to use these from a common location (they are in infrastructure currently)
    private static final String REPLICATION_EVENT_TABLE_NAME = "LogReplicationEventTable";

    private static final String LR_STREAM_TAG = "log_replication";

    private Table<Queue.CorfuGuidMsg, RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> logEntryQ;
    private Table<Queue.CorfuGuidMsg, RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> snapSyncQ;
    private Table<Queue.RoutingQSnapStartEndKeyMsg, Queue.RoutingQSnapStartEndMarkerMsg, Message> snapStartEndTable;
    private Table<LogReplication.ReplicationEventInfoKey, LogReplication.ReplicationEvent, Message> replicationEventTable;

    private FullSyncRequestor fullSyncRequestor;

    private long lrMetadataSize = 0;
    private long totalPayloadSize = 0;
    private static final int INT_SIZE_IN_BYTES = 4;

    /**
     * Constructor for the log replication client for routing queues on sender.
     *
     * @param corfuStore CorfuStore instance used by the jvm
     * @param clientName String representation of the client name. This parameter is case-sensitive.
     * @throws IllegalArgumentException If clientName is null or empty.
     * @throws NoSuchMethodException    NoSuchMethodException.
     * @throws IllegalAccessException   IllegalAccessException.
     */
    public RoutingQueueSenderClient(CorfuStore corfuStore, String clientName)
            throws NoSuchMethodException, IllegalAccessException {
        Preconditions.checkArgument(isValid(clientName), "clientName is null or empty.");

        this.corfuStore = corfuStore;
        this.clientName = clientName;

        int numOpenQueueRetries = 8;
        Table<Queue.CorfuGuidMsg, RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> logEntryQ_local = null;
        Table<Queue.CorfuGuidMsg, RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> snapSyncQ_local = null;
        Table<Queue.RoutingQSnapStartEndKeyMsg, Queue.RoutingQSnapStartEndMarkerMsg, Message> snapStartEnd_local = null;
        while ((numOpenQueueRetries--) > 0) {
            try {
                logEntryQ_local = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, LOG_ENTRY_SYNC_QUEUE_NAME_SENDER,
                        RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(RoutingTableEntryMsg.class));
                snapSyncQ_local = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, SNAPSHOT_SYNC_QUEUE_NAME_SENDER,
                        RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(RoutingTableEntryMsg.class));
                snapStartEnd_local = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, SNAP_SYNC_START_END_Q_NAME,
                        Queue.RoutingQSnapStartEndKeyMsg.class, Queue.RoutingQSnapStartEndMarkerMsg.class, null,
                        TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));

                break;
            } catch (InvocationTargetException e) {
                throw new RuntimeException("InvocationTargetException in fromProtoSchema" + e.getMessage());
            } catch (TransactionAbortedException e) {
                log.warn("OpenQueue in RoutingQSender hit TAE: retry" + numOpenQueueRetries);
            } catch (StreamingException se) {
                log.warn("RoutingQSender subscription hit a Streaming Exception retrying " + numOpenQueueRetries);
            }
        }

        while ((numOpenQueueRetries--) > 0) {
            try {
                replicationEventTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, REPLICATION_EVENT_TABLE_NAME,
                        LogReplication.ReplicationEventInfoKey.class,
                        LogReplication.ReplicationEvent.class,
                        null,
                        TableOptions.fromProtoSchema(LogReplication.ReplicationEvent.class));
                break;
            } catch (InvocationTargetException | IllegalArgumentException e) {
                log.error("Failed to open the Event Table", e);
                throw new RuntimeException(e);
            }
        }

        this.logEntryQ = logEntryQ_local;
        this.snapSyncQ = snapSyncQ_local;
        this.snapStartEndTable = snapStartEnd_local;

        // TODO: Register this client once the DEFAULT CLIENT implementation is no longer needed
        // register(corfuStore, clientName);
    }

    public void startLRSnapshotTransmitter(LRTransmitterReplicationModule snapSyncProvider) {
        this.fullSyncRequestor = new FullSyncRequestor(
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

    private class FullSyncRequestor extends StreamListenerResumeOrDefault {

        private final LRTransmitterReplicationModule snapSyncProvider;

        public FullSyncRequestor(CorfuStore store, LRTransmitterReplicationModule snapSyncProvider,
                                 String namespace, String streamTag, List<String> tablesOfInterest) {
            super(store, namespace, streamTag, tablesOfInterest);
            this.snapSyncProvider = snapSyncProvider;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            // TODO: Check for existing events. What if subscription comes later than the event is published.
            log.info("onNext[{}] :: got updates on RoutingQSender for tables {}", results.getTimestamp(),
                    results.getEntries().keySet().stream().map(TableSchema::getTableName).collect(Collectors.toList()));
            LogReplication.ReplicationEvent fullSyncEvent = null;
            LogReplication.ReplicationEventInfoKey key = null;
            // Any notification here indicates a full sync request
            for (List<CorfuStreamEntry> entryList : results.getEntries().values()) {
                for (CorfuStreamEntry entry : entryList) {
                    if (entry.getOperation() == CorfuStreamEntry.OperationType.CLEAR) {
                        log.warn("RoutingQEventListener ignoring a CLEAR operation");
                        continue;
                    }
                    key = (LogReplication.ReplicationEventInfoKey) entry.getKey();
                    fullSyncEvent = (LogReplication.ReplicationEvent) entry.getPayload();
                    if (fullSyncEvent.getType() !=
                            LogReplication.ReplicationEvent.ReplicationEventType.SERVER_REQUESTED_SNAPSHOT_SYNC) {
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
                snapSyncProvider.provideFullStateData(snapshotSyncDataTransmitter);
            }
        }
    }

    private class SnapshotSyncDataTransmitter implements LRFullStateReplicationContext {

        @Getter
        @Setter
        ScopedTransaction snapshot = null;

        private boolean baseSnapshotSent;

        private final LogReplication.ReplicationEvent requestingEvent;

        LogReplication.ReplicationEventInfoKey key;

        public SnapshotSyncDataTransmitter(LogReplication.ReplicationEvent requestingEvent,
                                           LogReplication.ReplicationEventInfoKey key) {
            this.requestingEvent = requestingEvent;
            this.baseSnapshotSent = false;
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
        public LogReplication.ReplicationEvent.ReplicationEventType getReason() {
            return requestingEvent.getType();
        }

        @Override
        public void transmit(RoutingTableEntryMsg message) throws CancellationException {
            transmit(message, 0);
        }

        @Override
        public void transmit(RoutingTableEntryMsg message, int progress) throws CancellationException {
            log.info("Enqueuing message to full sync queue, message: {}", message);
            getTxn().logUpdateEnqueue(snapSyncQ, message, message.getDestinationsList().stream()
                    .map(destination -> TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                            SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + destination))
                    .collect(Collectors.toList()), corfuStore);

            long metadataSize = message.getSerializedSize() -
                    (message.getOpaquePayload().size() + message.getOpaqueMetadata().size() + INT_SIZE_IN_BYTES);

            lrMetadataSize += metadataSize;
            totalPayloadSize += message.getSerializedSize();

            if (!baseSnapshotSent) {
                long baseSnapshotToPick;
                if (snapshot != null) {
                    baseSnapshotToPick = snapshot.getTxnSnapshot().getSequence();
                    log.info("FullSync base snapshot from Scoped Transaction = {}", baseSnapshotToPick);
                } else {
                    baseSnapshotToPick = getTxn().getTxnSequence();
                    log.info("FullSync base snapshot from first full sync transaction = {}", baseSnapshotToPick);
                }
                Queue.RoutingQSnapStartEndKeyMsg keyOfStartMarker = Queue.RoutingQSnapStartEndKeyMsg.newBuilder()
                        .setSnapshotSyncId(requestingEvent.getEventId()).build();
                Queue.RoutingQSnapStartEndMarkerMsg startMarker = Queue.RoutingQSnapStartEndMarkerMsg.newBuilder()
                    .setSnapshotStartTimestamp(baseSnapshotToPick) // This is the base snapshot timestamp
                    .setDestination(key.getSession().getSinkClusterId()).build();

                CorfuRecord<Queue.RoutingQSnapStartEndMarkerMsg, Message> markerEntry =
                        new CorfuRecord<>(startMarker, null);

                Object[] smrArgs = new Object[2];
                smrArgs[0] = keyOfStartMarker;
                smrArgs[1] = markerEntry;
                getTxn().logUpdate(LogReplicationUtils.lrSnapStartEndQId,
                        new SMREntry("put", smrArgs,
                                corfuStore.getRuntime().getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE)),
                        message.getDestinationsList().stream()
                                .map(destination -> TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                                        SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + destination))
                                .collect(Collectors.toList())
                );
                log.info("Base snapshot marker key {}, value {}", keyOfStartMarker, startMarker);
                baseSnapshotSent = true;
            }
        }

        @Override
        public void markCompleted() throws CancellationException {
            log.info("Got completion marker");
            Queue.RoutingQSnapStartEndKeyMsg keyOfStartMarker = Queue.RoutingQSnapStartEndKeyMsg.newBuilder()
                .setSnapshotSyncId(requestingEvent.getEventId()).build();
            Queue.RoutingQSnapStartEndMarkerMsg startMarker = Queue.RoutingQSnapStartEndMarkerMsg.newBuilder()
                    .setSnapshotStartTimestamp(0) // Empty snapshot timestamp signifies an end marker
                    .setDestination(key.getSession().getSinkClusterId()).build();

            CorfuRecord<Queue.RoutingQSnapStartEndMarkerMsg, Message> markerEntry =
                    new CorfuRecord<>(startMarker, null);

            Object[] smrArgs = new Object[2];
            smrArgs[0] = keyOfStartMarker;
            smrArgs[1] = markerEntry;

            try {
                IRetry.build(IntervalRetry.class, () -> {
                    try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                        txnContext.logUpdate(LogReplicationUtils.lrSnapStartEndQId,
                                new SMREntry("put", smrArgs,
                                        corfuStore.getRuntime().getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE)),
                                Arrays.asList(TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                                        SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + key.getSession().getSinkClusterId()))
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

            log.info("Total Payload Size = {}.  LR Metadata Size = {}", totalPayloadSize, lrMetadataSize);
            totalPayloadSize = 0;
            lrMetadataSize = 0;
            if (getSnapshot() != null) {
                getSnapshot().close();
                setSnapshot(null);
            }
        }

        @Override
        public void cancel() {
            // TODO: Need to figure out what might be LR's equivalent of a full sync cancellation?
            if (getSnapshot() != null) {
                getSnapshot().close();
                setSnapshot(null);
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
        log.info("Enqueuing message to delta queue, message: {}", message);
        txn.logUpdateEnqueue(logEntryQ, message, message.getDestinationsList().stream()
                .map(destination -> {
                    UUID streamIdForTag = TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                            LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + destination);
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
     * Request LR to perform a forced snapshot sync for a specific remote site.
     *
     * @param sourceClusterId Id of the Source Cluster
     * @param sinkClusterId   Id of the Sink Cluster
     */
    public void requestSnapshotSync(UUID sourceClusterId, UUID sinkClusterId) {
        requestSnapshotSync(sourceClusterId, sinkClusterId,
                LogReplication.ReplicationEvent.ReplicationEventType.CLIENT_REQUESTED_FORCED_SNAPSHOT_SYNC);
    }

    /**
     * Request LR to perform a forced snapshot sync for ALL remote sites (To be used for testing only)
     *
     * @param sourceClusterId Id of the Source Cluster
     * @param sinkClusterId   Id of the Sink Cluster
     */
    public void requestGlobalSnapshotSync(UUID sourceClusterId, UUID sinkClusterId) {
        log.warn("Requesting a full sync for ALL remote sites at once!");
        requestSnapshotSync(sourceClusterId, sinkClusterId,
                LogReplication.ReplicationEvent.ReplicationEventType.UPGRADE_COMPLETION_FORCE_SNAPSHOT_SYNC);
    }

    /**
     * Request LR to perform a forced snapshot sync.
     *
     * @param sourceClusterId Id of the Source Cluster
     * @param sinkClusterId   Id of the Sink Cluster
     * @param replicationEventType request one or all
     */
    public void requestSnapshotSync(UUID sourceClusterId, UUID sinkClusterId,
                                    LogReplication.ReplicationEvent.ReplicationEventType replicationEventType) {
        LogReplication.ReplicationSubscriber subscriber = LogReplication.ReplicationSubscriber.newBuilder()
                .setClientName(this.clientName)
                .setModel(ReplicationModel.ROUTING_QUEUES)
                .build();
        LogReplication.LogReplicationSession session =
                LogReplication.LogReplicationSession.newBuilder()
                        .setSourceClusterId(sourceClusterId.toString())
                        .setSinkClusterId(sinkClusterId.toString())
                        .setSubscriber(subscriber).build();
        enforceSnapshotSync(session, replicationEventType);

    }

    // TODO pankti:  Move SnapshotSyncUtils to the 'runtime' package so that this method can be reused from there
    private void enforceSnapshotSync(LogReplication.LogReplicationSession session,
                                     LogReplication.ReplicationEvent.ReplicationEventType eventType) {
        UUID forceSyncId = UUID.randomUUID();

        log.info("Forced snapshot sync will be triggered, session={}, sync_id={}", session, forceSyncId);

        // Write a force sync event to the logReplicationEventTable
        LogReplication.ReplicationEventInfoKey key = LogReplication.ReplicationEventInfoKey.newBuilder()
                .setSession(session)
                .build();

        LogReplication.ReplicationEvent event = LogReplication.ReplicationEvent.newBuilder()
                .setEventId(forceSyncId.toString())
                .setType(eventType)
                .setEventTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .build();

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
}
