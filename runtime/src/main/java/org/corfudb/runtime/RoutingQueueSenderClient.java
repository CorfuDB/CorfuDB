package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.LogReplication.ReplicationEvent;
import org.corfudb.runtime.Queue.RoutingQSnapStartEndKeyMsg;
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListenerResumeOrDefault;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.ProtobufSerializer;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.LogReplicationUtils.SNAP_SYNC_START_END_Q_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class RoutingQueueSenderClient extends LogReplicationClient implements LogReplicationRoutingQueueClient {
    // TODO (V2): This field should be removed after the rpc stream is added for Sink side session creation.
    public static final String DEFAULT_ROUTING_QUEUE_CLIENT = "00000000-0000-0000-0000-0000000000002";

    // TODO: Find a way to use these from a common location (they are in infrastructure currently)
    private static final String REPLICATION_EVENT_TABLE_NAME = "LogReplicationEventTable";
    private static final String LR_STREAM_TAG = "log_replication";
    private final CorfuStore corfuStore;
    private final String clientName;

    private Table<Queue.CorfuGuidMsg, RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> logEntryQ;
    private Table<Queue.CorfuGuidMsg, RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> snapSyncQ;
    private Table<RoutingQSnapStartEndKeyMsg, Queue.RoutingQSnapStartEndMarkerMsg, Message> snapStartEndTable;

    /**
     * Constructor for the log replication client for routing queues on sender.
     *
     * @param corfuStore CorfuStore instance used by the jvm
     * @param clientName String representation of the client name. This parameter is case-sensitive.
     * @param snapSyncProvider Callback implemented by application that transmits full snap sync data to LR
     * @throws IllegalArgumentException If clientName is null or empty.
     * @throws NoSuchMethodException NoSuchMethodException.
     * @throws IllegalAccessException IllegalAccessException.
     */
    public RoutingQueueSenderClient(CorfuStore corfuStore, String clientName,
                                        LRTransmitterReplicationModule snapSyncProvider)
            throws NoSuchMethodException, IllegalAccessException {
        Preconditions.checkArgument(isValid(clientName), "clientName is null or empty.");

        this.corfuStore = corfuStore;
        this.clientName = clientName;

        int numOpenQueueRetries = 8;
        Table<Queue.CorfuGuidMsg, RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> logEntryQ_local = null;
        Table<Queue.CorfuGuidMsg, RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> snapSyncQ_local = null;
        Table<RoutingQSnapStartEndKeyMsg, Queue.RoutingQSnapStartEndMarkerMsg, Message> snapStartEnd_local = null;
        while ((numOpenQueueRetries--) > 0) {
            try {
                logEntryQ_local = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, LOG_ENTRY_SYNC_QUEUE_NAME_SENDER,
                        RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(RoutingTableEntryMsg.class));
                snapSyncQ_local = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, SNAPSHOT_SYNC_QUEUE_NAME_SENDER,
                        RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(RoutingTableEntryMsg.class));
                snapStartEnd_local = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, SNAP_SYNC_START_END_Q_NAME,
                        RoutingQSnapStartEndKeyMsg.class, Queue.RoutingQSnapStartEndMarkerMsg.class, null,
                        TableOptions.fromProtoSchema(Queue.RoutingTableEntryMsg.class));

                FullSyncRequestor fullSyncRequestor = new FullSyncRequestor(
                        corfuStore, snapSyncProvider, CORFU_SYSTEM_NAMESPACE,
                        LR_STREAM_TAG, Collections.singletonList(REPLICATION_EVENT_TABLE_NAME));

                corfuStore.subscribeListener(fullSyncRequestor, CORFU_SYSTEM_NAMESPACE,
                        LR_STREAM_TAG, Collections.singletonList(REPLICATION_EVENT_TABLE_NAME));

                break;
            } catch (InvocationTargetException e) {
                throw new RuntimeException("InvocationTargetException in fromProtoSchema"+ e.getMessage());
            } catch (TransactionAbortedException e) {
                log.warn("OpenQueue in RoutingQSender hit TAE: retry"+numOpenQueueRetries);
            } catch (StreamingException se) {
                log.warn("RoutingQSender subscription hit a Streaming Exception retrying "+numOpenQueueRetries);
            }
        }
        this.logEntryQ = logEntryQ_local;
        this.snapSyncQ = snapSyncQ_local;
        this.snapStartEndTable = snapStartEnd_local;

        // TODO: Register this client once the DEFAULT CLIENT implementation is no longer needed
        // register(corfuStore, clientName);
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
            log.info("onNext[{}] :: got updates on RoutingQSender for tables {}", results.getTimestamp(),
                    results.getEntries().keySet().stream().map(TableSchema::getTableName).collect(Collectors.toList()));
            ReplicationEvent fullSyncEvent = null;
            // Any notification here indicates a full sync request
            for (List<CorfuStreamEntry> entryList : results.getEntries().values()) {
                for (CorfuStreamEntry entry : entryList) {
                    if (entry.getOperation() == CorfuStreamEntry.OperationType.CLEAR) {
                        log.warn("RoutingQEventListener ignoring a CLEAR operation");
                        continue;
                    }
                    LogReplication.ReplicationEventInfoKey key = (LogReplication.ReplicationEventInfoKey) entry.getKey();
                    fullSyncEvent = (ReplicationEvent) entry.getPayload();
                    log.info("Full Sync requested due to event :: id={}, type={}, session={}, ts={}",
                            fullSyncEvent.getEventId(), fullSyncEvent.getType(),
                            key.getSession(), fullSyncEvent.getEventTimestamp());
                }
            }
            if (fullSyncEvent != null) {
                SnapshotSyncDataTransmitter snapshotSyncDataTransmitter = new SnapshotSyncDataTransmitter(
                        fullSyncEvent);
                snapSyncProvider.provideFullStateData(snapshotSyncDataTransmitter);
            }
        }
    }

    private class SnapshotSyncDataTransmitter implements LRFullStateReplicationContext {
        private boolean baseSnapshotSent;

        private final ReplicationEvent requestingEvent;
        public SnapshotSyncDataTransmitter(ReplicationEvent requestingEvent) {
            this.requestingEvent = requestingEvent;
            this.baseSnapshotSent = false;
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
            return requestingEvent.getClusterId();
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
            log.info("Enqueuing message to full sync queue, message: {}", message);
            getTxn().logUpdateEnqueue(snapSyncQ, message, message.getDestinationsList().stream()
                    .map(destination -> TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                            SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + destination))
                    .collect(Collectors.toList()), corfuStore);
            if (!baseSnapshotSent) { // Set the FIRST FULL SYNC TRANSACTION's snapshot as base snapshot for LR sync
                RoutingQSnapStartEndKeyMsg keyOfStartMarker = RoutingQSnapStartEndKeyMsg.newBuilder()
                        .setSnapshotSyncId(requestingEvent.getEventId()).build();
                Queue.RoutingQSnapStartEndMarkerMsg startMarker = Queue.RoutingQSnapStartEndMarkerMsg.newBuilder()
                        .setSnapshotStartTimestamp(getTxn().getTxnSequence())
                        .setDestination(requestingEvent.getClusterId()).build();

                CorfuRecord<RoutingQSnapStartEndKeyMsg, Queue.RoutingQSnapStartEndMarkerMsg> markerEntry =
                        new CorfuRecord<>(keyOfStartMarker, startMarker);

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
                baseSnapshotSent = true;
            }
        }

        @Override
        public void markCompleted() throws CancellationException {
            log.info("Got completion marker");
            RoutingQSnapStartEndKeyMsg keyOfStartMarker = RoutingQSnapStartEndKeyMsg.newBuilder()
                    .setSnapshotSyncId(requestingEvent.getEventId()).build();
            Queue.RoutingQSnapStartEndMarkerMsg startMarker = Queue.RoutingQSnapStartEndMarkerMsg.newBuilder()
                    .setDestination(requestingEvent.getClusterId()).build();

            CorfuRecord<RoutingQSnapStartEndKeyMsg, Queue.RoutingQSnapStartEndMarkerMsg> markerEntry =
                    new CorfuRecord<>(keyOfStartMarker, startMarker);

            Object[] smrArgs = new Object[2];
            smrArgs[0] = keyOfStartMarker;
            smrArgs[1] = markerEntry;
            getTxn().logUpdate(LogReplicationUtils.lrSnapStartEndQId,
                    new SMREntry("put", smrArgs,
                            corfuStore.getRuntime().getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE)),
                    Arrays.asList(TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                            SNAPSHOT_SYNC_QUEUE_TAG_SENDER_PREFIX + requestingEvent.getClusterId()))
            );
        }

        @Override
        public void cancel() {
            // TODO: Need to figure out what might be LR's equivalent of a full sync cancellation?
        }
    }

    /**
     * Enqueues message to be replicated onto the sender's delta queue.
     *
     * @param txn Transaction context in which the operation will be performed
     * @param message RoutingTableEntryMsg
     */
    @Override
    public void transmitDeltaMessage(TxnContext txn, RoutingTableEntryMsg message, CorfuStore corfuStore) throws Exception {
        log.info("Enqueuing message to delta queue, message: {}", message);
        txn.logUpdateEnqueue(logEntryQ, message, message.getDestinationsList().stream()
                .map(destination -> TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                        LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + destination))
                .collect(Collectors.toList()), corfuStore);
    }

    /**
     * Enqueues messages to be replicated onto the sender's delta queue.
     *
     * @param txn Transaction context in which the operation will be performed
     * @param messages List of RoutingTableEntryMsg
     */
    @Override
    public void transmitDeltaMessages(TxnContext txn, List<RoutingTableEntryMsg> messages, CorfuStore corfuStore) throws Exception {
        for (RoutingTableEntryMsg message : messages) {
            log.info("Enqueuing message to delta queue, message: {}", message);
            txn.logUpdateEnqueue(logEntryQ, message, message.getDestinationsList().stream()
                    .map(destination -> TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                            LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + destination))
                    .collect(Collectors.toList()), corfuStore);
        }
    }

    /**
     * Request LR to perform a forced snapshot sync.
     *
     * @param timestamp Timestamp from which recovery is possible.
     */
    public void requestSnapshotSync(Timestamp timestamp) {

    }
}
