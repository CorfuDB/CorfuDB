package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.TableRegistry;

import java.util.List;
import java.util.stream.Collectors;

import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class RoutingQueueSenderClient extends LogReplicationClient implements LogReplicationRoutingQueueClient {
    private static final ReplicationModel model = ReplicationModel.ROUTING_QUEUES;

    // TODO (V2): This field should be removed after the rpc stream is added for Sink side session creation.
    public static final String DEFAULT_ROUTING_QUEUE_CLIENT = "00000000-0000-0000-0000-0000000000002";

    private Table<Queue.CorfuGuidMsg, RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> logEntryQ;

    public RoutingQueueSenderClient() {
        // TODO: This might be removed in the future. Temporary solution for bypassing providing a CorfuRuntime
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
        try {
            log.info("Get log entry sync queue: {}", LOG_ENTRY_SYNC_QUEUE_NAME_SENDER);
            logEntryQ = txn.getTable(LOG_ENTRY_SYNC_QUEUE_NAME_SENDER);
        } catch (IllegalStateException e) {
            log.info("Log entry sync queue not opened yet, opening it now!");
            logEntryQ = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, LOG_ENTRY_SYNC_QUEUE_NAME_SENDER,
                    RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(RoutingTableEntryMsg.class));
        }

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
        try {
            log.info("Get log entry sync queue: {}", LOG_ENTRY_SYNC_QUEUE_NAME_SENDER);
            logEntryQ = txn.getTable(LOG_ENTRY_SYNC_QUEUE_NAME_SENDER);
        } catch (IllegalStateException e) {
            log.info("Log entry sync queue not opened yet, opening it now!");
            logEntryQ = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, LOG_ENTRY_SYNC_QUEUE_NAME_SENDER,
                    RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(RoutingTableEntryMsg.class));
        }

        for (RoutingTableEntryMsg message : messages) {
            log.info("Enqueuing message to delta queue, message: {}", message);
            txn.logUpdateEnqueue(logEntryQ, message, message.getDestinationsList().stream()
                    .map(destination -> {
                        log.info("Stream tag ID: {}", TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                                LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + destination));
                        return TableRegistry.getStreamIdForStreamTag(CORFU_SYSTEM_NAMESPACE,
                                LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + destination);
                    })
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
