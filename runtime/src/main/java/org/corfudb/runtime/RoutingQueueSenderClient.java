package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.TableRegistry;

import java.lang.reflect.InvocationTargetException;
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
    private CorfuStore corfuStore;

    public RoutingQueueSenderClient() {
        // TODO: This might be removed in the future. Temporary solution for bypassing providing a CorfuRuntime
    }

    /**
     * Constructor for the log replication client for routing queues on sender.
     *
     * @param runtime Corfu Runtime.
     * @param clientName String representation of the client name. This parameter is case-sensitive.
     * @throws IllegalArgumentException If clientName is null or empty.
     * @throws InvocationTargetException InvocationTargetException.
     * @throws NoSuchMethodException NoSuchMethodException.
     * @throws IllegalAccessException IllegalAccessException.
     */
    public RoutingQueueSenderClient(CorfuRuntime runtime, String clientName)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        Preconditions.checkArgument(isValid(clientName), "clientName is null or empty.");

        this.corfuStore = new CorfuStore(runtime);
        logEntryQ = corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, LOG_ENTRY_SYNC_QUEUE_NAME_SENDER,
                RoutingTableEntryMsg.class, TableOptions.fromProtoSchema(RoutingTableEntryMsg.class));

        register(corfuStore, clientName, model);
    }

    /**
     * Enqueues message to be replicated onto the sender's delta queue.
     *
     * @param txn Transaction context in which the operation will be performed
     * @param message RoutingTableEntryMsg
     */
    @Override
    public void transmitDeltaMessage(TxnContext txn, RoutingTableEntryMsg message) {
        log.info("Enqueuing message to delta queue, message: {}", message);
        txn.logUpdateEnqueue(logEntryQ, message, message.getDestinationsList().stream()
                .map(destination -> TableRegistry.getStreamIdForStreamTag(DEMO_NAMESPACE, LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + destination))
                .collect(Collectors.toList()), corfuStore);
    }

    /**
     * Enqueues messages to be replicated onto the sender's delta queue.
     *
     * @param txn Transaction context in which the operation will be performed
     * @param messages List of RoutingTableEntryMsg
     */
    @Override
    public void transmitDeltaMessages(TxnContext txn, List<RoutingTableEntryMsg> messages) {
        for (RoutingTableEntryMsg message : messages) {
            log.info("Enqueuing message to delta queue, message: {}", message);
            txn.logUpdateEnqueue(logEntryQ, message, message.getDestinationsList().stream()
                    .map(destination -> TableRegistry.getStreamIdForStreamTag(DEMO_NAMESPACE, LOG_ENTRY_SYNC_QUEUE_TAG_SENDER_PREFIX + destination))
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
