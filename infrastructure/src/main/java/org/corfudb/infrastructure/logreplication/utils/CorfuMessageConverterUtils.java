package org.corfudb.infrastructure.logreplication.utils;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationLeadershipLoss;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;
import org.corfudb.runtime.Messages;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.Messages.LogReplicationEntryMetadata;
import org.corfudb.runtime.Messages.CorfuMessageType;
import org.corfudb.utils.common.CorfuMessageProtoBufException;

import java.util.UUID;

/**
 * Utility class to convert between legacy Corfu Messages and ProtoBuf messages
 */
public class CorfuMessageConverterUtils {

    /**
     * Convert between legacy Java CorfuMsg to protoBuf definition.
     *
     * @param msg legacy corfu message
     * @return protoBuf message definition or null if message is not supported.
     */
    public static CorfuMessage toProtoBuf(CorfuMsg msg) {

        CorfuMessage.Builder protoCorfuMsg = CorfuMessage.newBuilder()
                .setClientID(Messages.Uuid.newBuilder().setLsb(msg.getClientID().getLeastSignificantBits())
                        .setMsb(msg.getClientID().getMostSignificantBits()).build())
                .setEpoch(msg.getEpoch())
                .setPriority(Messages.CorfuPriorityLevel.valueOf(msg.getPriorityLevel().name()))
                .setRequestID(msg.getRequestID());

        switch (msg.getMsgType()) {
            case LOG_REPLICATION_ENTRY:
                CorfuPayloadMsg<LogReplicationEntry> entry = (CorfuPayloadMsg<LogReplicationEntry>) msg;
                LogReplicationEntry logReplicationEntry = entry.getPayload();
                return protoCorfuMsg
                        .setType(CorfuMessageType.LOG_REPLICATION_ENTRY)
                        // Set Log Replication Entry as payload
                        .setPayload(Any.pack(Messages.LogReplicationEntry.newBuilder()
                                .setMetadata(LogReplicationEntryMetadata.newBuilder()
                                        .setSiteConfigID(logReplicationEntry.getMetadata().getTopologyConfigId())
                                        .setType(Messages.LogReplicationEntryType.valueOf(logReplicationEntry.getMetadata().getMessageMetadataType().name()))
                                        .setPreviousTimestamp(logReplicationEntry.getMetadata().getPreviousTimestamp())
                                        .setSnapshotSyncSeqNum(logReplicationEntry.getMetadata().getSnapshotSyncSeqNum())
                                        .setSnapshotTimestamp(logReplicationEntry.getMetadata().getSnapshotTimestamp())
                                        .setSyncRequestId(Messages.Uuid.newBuilder().setMsb(logReplicationEntry.getMetadata().getSyncRequestId().getMostSignificantBits())
                                                .setLsb(logReplicationEntry.getMetadata().getSyncRequestId().getLeastSignificantBits()).build())
                                        .setTimestamp(logReplicationEntry.getMetadata().getTimestamp()))
                                .setData(ByteString.copyFrom(logReplicationEntry.getPayload()))
                                .build()))
                        .build();
            case LOG_REPLICATION_METADATA_RESPONSE:
                CorfuPayloadMsg<LogReplicationMetadataResponse> corfuMsg = (CorfuPayloadMsg<LogReplicationMetadataResponse>) msg;
                LogReplicationMetadataResponse negotiationResponse = corfuMsg.getPayload();
                return protoCorfuMsg
                        .setType(Messages.CorfuMessageType.LOG_REPLICATION_METADATA_RESPONSE)
                        .setPayload(Any.pack(Messages.LogReplicationMetadataResponse.newBuilder()
                                .setSiteConfigID(negotiationResponse.getTopologyConfigId())
                                .setVersion(negotiationResponse.getVersion())
                                .setSnapshotStart(negotiationResponse.getSnapshotStart())
                                .setSnapshotTransferred(negotiationResponse.getSnapshotTransferred())
                                .setSnapshotApplied(negotiationResponse.getSnapshotApplied())
                                .setLastLogEntryTimestamp(negotiationResponse.getLastLogProcessed())
                                .build()))
                        .build();
            case LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE:
                CorfuPayloadMsg<LogReplicationQueryLeaderShipResponse> corfuPayloadMsg = (CorfuPayloadMsg<LogReplicationQueryLeaderShipResponse>) msg;
                LogReplicationQueryLeaderShipResponse leaderShipResponse = corfuPayloadMsg.getPayload();
                return protoCorfuMsg
                        .setType(Messages.CorfuMessageType.LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE)
                        .setPayload(Any.pack(Messages.LogReplicationQueryLeadershipResponse.newBuilder()
                                .setEpoch(leaderShipResponse.getEpoch())
                                .setIsLeader(leaderShipResponse.isLeader())
                                .setNodeId(leaderShipResponse.getNodeId())
                                .build()))
                        .build();
            case LOG_REPLICATION_METADATA_REQUEST:
                return protoCorfuMsg
                        .setType(Messages.CorfuMessageType.LOG_REPLICATION_METADATA_REQUEST)
                        .build();
            case LOG_REPLICATION_QUERY_LEADERSHIP:
                return protoCorfuMsg
                        .setType(Messages.CorfuMessageType.LOG_REPLICATION_QUERY_LEADERSHIP)
                        .build();
            case LOG_REPLICATION_LEADERSHIP_LOSS:
                LogReplicationLeadershipLoss leadershipLoss = ((CorfuPayloadMsg<LogReplicationLeadershipLoss>) msg).getPayload();
                return protoCorfuMsg
                        .setType(CorfuMessageType.LOG_REPLICATION_LEADERSHIP_LOSS)
                        .setPayload(Any.pack(Messages.LogReplicationLeadershipLoss.newBuilder()
                                .setNodeId(leadershipLoss.getNodeId())
                                .build()))
                        .build();
            default:
                throw new IllegalArgumentException(String.format("{} type is not supported", msg.getMsgType()));
        }
    }

    /**
     * Convert from protoBuf definition to legacy Java Corfu Message
     *
     * @param protoMessage protoBuf message
     * @return legacy java corfu message
     */
    public static CorfuMsg fromProtoBuf(CorfuMessage protoMessage) throws CorfuMessageProtoBufException {

        // Base Corfu Message
        UUID clientId = new UUID(protoMessage.getClientID().getMsb(), protoMessage.getClientID().getLsb());
        long requestId = protoMessage.getRequestID();
        PriorityLevel priorityLevel = PriorityLevel.fromProtoType(protoMessage.getPriority());
        ByteBuf buf = Unpooled.wrappedBuffer(protoMessage.getPayload().toByteArray());
        long epoch = protoMessage.getEpoch();

        try {
            switch (protoMessage.getType()) {
                case LOG_REPLICATION_ENTRY:
                    LogReplicationEntry logReplicationEntry = LogReplicationEntry
                            .fromProto(protoMessage.getPayload().unpack(Messages.LogReplicationEntry.class));

                    return new CorfuPayloadMsg<>(CorfuMsgType.LOG_REPLICATION_ENTRY, logReplicationEntry)
                            .setClientID(clientId)
                            .setRequestID(requestId)
                            .setPriorityLevel(priorityLevel)
                            .setBuf(buf)
                            .setEpoch(epoch);
                case LOG_REPLICATION_METADATA_REQUEST:
                    return new CorfuMsg(clientId, null, requestId, epoch, null,
                            CorfuMsgType.LOG_REPLICATION_METADATA_REQUEST, priorityLevel);
                case LOG_REPLICATION_QUERY_LEADERSHIP:
                    return new CorfuMsg(clientId, null, requestId, epoch, null,
                            CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP, priorityLevel);
                case LOG_REPLICATION_METADATA_RESPONSE:
                    LogReplicationMetadataResponse negotiationResponse = LogReplicationMetadataResponse
                            .fromProto(protoMessage.getPayload().unpack(Messages.LogReplicationMetadataResponse.class));

                    return new CorfuPayloadMsg<>(CorfuMsgType.LOG_REPLICATION_METADATA_RESPONSE, negotiationResponse)
                            .setClientID(clientId)
                            .setRequestID(requestId)
                            .setPriorityLevel(priorityLevel)
                            .setBuf(buf)
                            .setEpoch(epoch);
                case LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE:
                    LogReplicationQueryLeaderShipResponse leadershipResponse = LogReplicationQueryLeaderShipResponse
                            .fromProto(protoMessage.getPayload().unpack(Messages.LogReplicationQueryLeadershipResponse.class));

                    return new CorfuPayloadMsg<>(CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE, leadershipResponse)
                            .setClientID(clientId)
                            .setRequestID(requestId)
                            .setPriorityLevel(priorityLevel)
                            .setBuf(buf)
                            .setEpoch(epoch);
                case LOG_REPLICATION_LEADERSHIP_LOSS:
                    LogReplicationLeadershipLoss leadershipLoss = LogReplicationLeadershipLoss
                            .fromProto(protoMessage.getPayload().unpack(Messages.LogReplicationLeadershipLoss.class));
                    return new CorfuPayloadMsg<>(CorfuMsgType.LOG_REPLICATION_LEADERSHIP_LOSS, leadershipLoss)
                            .setClientID(clientId)
                            .setRequestID(requestId)
                            .setPriorityLevel(priorityLevel)
                            .setBuf(buf)
                            .setEpoch(epoch);
                default:
                    throw new IllegalArgumentException(String.format("%s type is not supported", protoMessage.getType().name()));
            }
        } catch (Exception e) {
            throw new CorfuMessageProtoBufException(e);
        }
    }

}
