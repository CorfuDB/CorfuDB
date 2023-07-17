package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
import org.corfudb.runtime.collections.CorfuRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_NAME;


/**
 * Log entry reader implementation for Routing Queues Replication Model.
 *
 * This implementation reads off the routing queue, a special data structure for this model, which holds
 * the data to be replicated.
 *
 */
@Slf4j
public class RoutingQueuesLogEntryReader extends BaseLogEntryReader {

    public RoutingQueuesLogEntryReader(CorfuRuntime runtime, LogReplicationSession session,
                                       LogReplicationContext replicationContext) {
        super(runtime, session, replicationContext);
    }

    @Override
    protected OpaqueEntry filterTransactionEntry(OpaqueEntry opaqueEntry) {
        List<SMREntry> routingTableEntryMsgs = opaqueEntry.getEntries().get(LogReplicationUtils.lrLogEntrySendQId);

        List<SMREntry> filteredMsgs = new ArrayList<>();

        for (SMREntry entry : routingTableEntryMsgs) {
            Object[] objs = entry.getSMRArguments();
            ByteBuf valueBuf = Unpooled.wrappedBuffer((byte[])objs[1]);
            CorfuRecord<RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg> record =
                (CorfuRecord<RoutingTableEntryMsg, Queue.CorfuQueueMetadataMsg>)
                    replicationContext.getProtobufSerializer().deserialize(valueBuf, null);

            if (record.getPayload().getDestinationsList().contains(session.getSinkClusterId())) {
                filteredMsgs.add(entry);
            }
        }
        HashMap<UUID, List<SMREntry>> opaqueEntryMap = new HashMap<>();
        opaqueEntryMap.put(CorfuRuntime.getStreamID(REPLICATED_QUEUE_NAME),
                filteredMsgs);
        return new OpaqueEntry(opaqueEntry.getVersion(), opaqueEntryMap);
    }

    @Override
    protected boolean isValidTransactionEntry(@NonNull OpaqueEntry entry) {
        Set<UUID> txEntryStreamIds = new HashSet<>(entry.getEntries().keySet());

        return txEntryStreamIds.contains(LogReplicationUtils.lrLogEntrySendQId);
    }
}
