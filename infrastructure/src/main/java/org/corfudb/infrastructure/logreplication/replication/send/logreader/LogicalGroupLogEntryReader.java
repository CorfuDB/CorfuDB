package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.config.LogReplicationLogicalGroupConfig;
import org.corfudb.infrastructure.logreplication.exceptions.GroupDestinationChangeException;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.ClientDestinationInfoKey;
import org.corfudb.runtime.LogReplication.DestinationInfoVal;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.ISerializer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.LogReplicationLogicalGroupClient.LR_MODEL_METADATA_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;


/**
 * Log entry reader implementation for Logical Grouping Replication Model.
 *
 * This implementation is very similar to the default implementation for the full table replication model,
 * with the exception that it will read from a different transactional stream for log entry sync (one that is
 * specific for this model).
 */
@Slf4j
public class LogicalGroupLogEntryReader extends BaseLogEntryReader {

    public static final UUID CLIENT_CONFIG_TABLE_ID = CorfuRuntime.getStreamID(
            TableRegistry.getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, LR_MODEL_METADATA_TABLE_NAME));

    private final ISerializer protobufSerializer;


    public LogicalGroupLogEntryReader(CorfuRuntime runtime, LogReplicationSession session,
                                      LogReplicationContext replicationContext) {
        super(runtime, session, replicationContext);
        protobufSerializer = replicationContext.getProtobufSerializer();
    }

    /**
     * Verify the transaction entry is valid. For LOGICAL_GROUP case, it will check:
     * (1) If current session is impacted by group destinations change
     * (2) If the opaque entry contains newly opened streams to replicate
     * <p>
     * Notice that a transaction stream entry can be fully or partially replicated,
     * i.e., if only a subset of streams in the transaction entry are part of the streams
     * to replicate, the transaction entry will be partially replicated,
     * avoiding replication of the other streams present in the transaction.
     *
     * @param entry transaction stream opaque entry
     * @return true, if the transaction entry has any valid stream to replicate.
     * false, otherwise.
     */
    @Override
    boolean isValidTransactionEntry(@NonNull OpaqueEntry entry) {
        Set<UUID> txEntryStreamIds = new HashSet<>(entry.getEntries().keySet());

        if (txEntryStreamIds.contains(CLIENT_CONFIG_TABLE_ID) &&
                isCurrentSessionImpacted(entry.getEntries().get(CLIENT_CONFIG_TABLE_ID))) {
            log.info("Group destination change detected, log entry sync will be stopped and a new snapshot sync " +
                    "will be triggered！");
            throw new GroupDestinationChangeException();
        }

        return super.isValidTransactionEntry(entry);
    }

    /**
     * Helper method for checking if the smr entries present in the opaque stream (that current session's log entry
     * reader is tracking) contain any group destination change with respect to current session. The opaque entries
     * will be deserialized to verify if the Sink cluster of current session is among the target destinations.
     *
     * @param groupConfigTableEntries SMREntries of LogReplicationModelMetadataTable.
     * @return True if current session is impacted by group destination config change, false otherwise.
     */
    private boolean isCurrentSessionImpacted(List<SMREntry> groupConfigTableEntries) {
        Set<String> groups = ((LogReplicationLogicalGroupConfig) replicationContext.getConfig(session))
                .getLogicalGroupToStreams().keySet();

        for (SMREntry smrEntry : groupConfigTableEntries) {
            // Get serialized form of arguments for registry table. They were sent in OpaqueEntry and
            // need to be deserialized using ProtobufSerializer
            Object[] objs = smrEntry.getSMRArguments();
            ByteBuf keyBuf = Unpooled.wrappedBuffer((byte[]) objs[0]);
            ClientDestinationInfoKey clientInfoKey = (ClientDestinationInfoKey) protobufSerializer
                    .deserialize(keyBuf, null);

            if (clientInfoKey.getModel() != session.getSubscriber().getModel() ||
                    !clientInfoKey.getClientName().equals(session.getSubscriber().getClientName())) {
                // SMREntry for other clients
                continue;
            }

            ByteBuf valueBuf = Unpooled.wrappedBuffer((byte[]) objs[1]);
            CorfuRecord<DestinationInfoVal, Message> destinations =
                    (CorfuRecord<DestinationInfoVal, Message>) protobufSerializer.deserialize(valueBuf, null);

            // From a session's point of view, there are 2 ways it could be impacted:
            // (1) a new group is added to have current session's Sink as its destination
            // (2) an existing group (already in config) no longer has current session's Sink as its destination
            boolean isGroupAdded = !groups.contains(clientInfoKey.getGroupName()) &&
                    destinations.getPayload().getDestinationIdsList().contains(session.getSinkClusterId());
            boolean isGroupRemoved = groups.contains(clientInfoKey.getGroupName()) &&
                    !destinations.getPayload().getDestinationIdsList().contains(session.getSinkClusterId());


            if (isGroupAdded || isGroupRemoved) {
                String groupChangeMessage = isGroupAdded ? "New group added for the Sink of current session. " :
                        "Group removed from the Sink of current session. ";
                log.info(groupChangeMessage + "Group=[{}], Sinks=[{}]", clientInfoKey.getGroupName(),
                        destinations.getPayload().getDestinationIdsList());

                return true;
            }
        }
        return false;
    }
}
