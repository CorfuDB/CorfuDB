package org.corfudb.runtime;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

@Slf4j
public class RoutingQueueSenderClient extends LogReplicationClient implements LogReplicationRoutingQueueClient{
    private static final ReplicationModel model = ReplicationModel.ROUTING_QUEUES;

    // TODO (V2): This field should be removed after the rpc stream is added for Sink side session creation.
    public static final String DEFAULT_ROUTING_QUEUE_CLIENT = "00000000-0000-0000-0000-0000000000002";

    private final CorfuStore corfuStore;

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
        register(corfuStore, clientName, model);
    }

    public void transmitDeltaMessage(TxnContext txn, RoutingTableEntryMsg message) {
    }

    public void transmitDeltaMessages(TxnContext txn, List<RoutingTableEntryMsg> messages) {
    }

    public void requestSnapshotSync(Timestamp timestamp) {
    }
}
