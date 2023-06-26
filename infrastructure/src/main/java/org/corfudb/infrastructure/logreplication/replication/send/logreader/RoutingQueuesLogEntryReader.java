package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplicationUtils;
import org.corfudb.runtime.Queue.RoutingTableEntryMsg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

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
    OpaqueEntry filterTransactionEntry(OpaqueEntry opaqueEntry) {
        List<SMREntry> routingTableEntryMsgs = opaqueEntry.getEntries().get(LogReplicationUtils.lrLogEntrySendQId);

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

    @Override
    boolean isValidTransactionEntry(@NonNull OpaqueEntry entry) {
        Set<UUID> txEntryStreamIds = new HashSet<>(entry.getEntries().keySet());
        Preconditions.checkState(txEntryStreamIds.size() == 1, "Routing queue session's log" +
                " entries should only come from the shared data queue for log entry sync");

        return txEntryStreamIds.contains(LogReplicationUtils.lrLogEntrySendQId);
    }
}
