package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;
import org.corfudb.runtime.exceptions.TrimmedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.corfudb.runtime.LogReplicationUtils.LOG_ENTRY_SYNC_QUEUE_NAME_SENDER;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_NAME_PREFIX;


/**
 * Log entry reader implementation for Routing Queues Replication Model.
 *
 * This implementation reads off the routing queue, a special data structure for this model, which holds
 * the data to be replicated.
 *
 */
public class RoutingQueuesLogEntryReader extends BaseLogEntryReader {

    public RoutingQueuesLogEntryReader(CorfuRuntime runtime, LogReplicationSession session,
                                       LogReplicationContext replicationContext) {
        super(runtime, session, replicationContext);
    }

    @Override
    public LogReplicationEntryMsg read(UUID logEntryRequestId) throws TrimmedException {
        return null;
    }

    @Override
    protected OpaqueEntry filterTransactionEntry(OpaqueEntry opaqueEntry) {
        List<SMREntry> routingTableEntryMsgs = opaqueEntry.getEntries()
            .get(CorfuRuntime.getStreamID(LOG_ENTRY_SYNC_QUEUE_NAME_SENDER));

        List<SMREntry> filteredMsgs = new ArrayList<>();

        for (SMREntry entry : routingTableEntryMsgs) {
            Object[] objs = entry.getSMRArguments();
            ByteBuf valueBuf = Unpooled.wrappedBuffer((byte[])objs[1]);
            RoutingTableEntryMsg msg =
                    (RoutingTableEntryMsg)replicationContext.getProtobufSerializer().deserialize(valueBuf, null);

            if (msg.getDestinationsList().contains(session.getSinkClusterId())) {
                filteredMsgs.add(entry);
            }
        }
        HashMap<UUID, List<SMREntry>> opaqueEntryMap = new HashMap<>();
        opaqueEntryMap.put(CorfuRuntime.getStreamID(REPLICATED_QUEUE_NAME_PREFIX + session.getSourceClusterId()),
                filteredMsgs);
        return new OpaqueEntry(opaqueEntry.getVersion(), opaqueEntryMap);
    }
}
