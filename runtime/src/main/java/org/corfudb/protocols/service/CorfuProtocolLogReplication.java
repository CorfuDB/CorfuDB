package org.corfudb.protocols.service;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

import java.util.ArrayList;
import java.util.List;

import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

/**
 * This class provides methods for creating some of the Protobuf objects defined
 * in log_replication.proto.
 */
@Slf4j
public final class CorfuProtocolLogReplication {
    // Prevent class from being instantiated
    private CorfuProtocolLogReplication() {}

    /**
     * Generate a log entry message.
     *
     * @param data     the actual data
     * @param metadata metadata describing the above data
     * @return         log entry message
     */
    public static LogReplicationEntryMsg getLrEntryMsg(ByteString data, LogReplicationEntryMetadataMsg metadata) {
        return LogReplicationEntryMsg.newBuilder()
                .setMetadata(metadata)
                .setData(data)
                .build();
    }

    /**
     * Generate a log entry ACK.
     *
     * @param metadata ACK metadata
     * @return         log entry ACK
     */
    public static LogReplicationEntryMsg getLrEntryAckMsg(LogReplicationEntryMetadataMsg metadata) {
        return LogReplication.LogReplicationEntryMsg.newBuilder()
                .setMetadata(metadata)
                .build();
    }

    /**
     * Given the metadata, override its sequencer number.
     *
     * @param metadata                    the metadata of the message
     * @param snapshotSyncSequenceNumber  sequencer number that will be used
     * @return                            a merged message
     */
    public static LogReplicationEntryMetadataMsg overrideSyncSeqNum(LogReplicationEntryMetadataMsg metadata,
                                                                    long snapshotSyncSequenceNumber) {
        return LogReplicationEntryMetadataMsg.newBuilder()
                .mergeFrom(metadata)
                .setSnapshotSyncSeqNum(snapshotSyncSequenceNumber).build();
    }


    /**
     * Given the metadata, override its sequencer number.
     *
     * @param metadata          the metadata of the message
     * @param topologyConfigId  topology config ID that will be used
     * @return                  a merged message
     */
    public static LogReplicationEntryMetadataMsg overrideTopologyConfigId(LogReplicationEntryMetadataMsg metadata,
                                                                          long topologyConfigId) {
        return LogReplicationEntryMetadataMsg.newBuilder()
                .mergeFrom(metadata)
                .setTopologyConfigID(topologyConfigId).build();
    }

    /**
     * Given the message, override its metadata.
     *
     * @param message   the rest of the message
     * @param metadata  the metadata of the message
     * @return          a merged message
     */
    public static LogReplicationEntryMsg overrideMetadata(LogReplicationEntryMsg message,
                                                          LogReplicationEntryMetadataMsg metadata) {
        return LogReplicationEntryMsg.newBuilder()
                .mergeFrom(message)
                .setMetadata(metadata)
                .build();
    }


    /**
     * Given a byte array, extract {@link OpaqueEntry}s.
     *
     * @param array  where to extract the data from
     * @return       list of extracted {@link OpaqueEntry}s
     */
    public static List<OpaqueEntry> extractOpaqueEntries(byte[] array) {
        ArrayList<OpaqueEntry> opaqueEntryList = new ArrayList<>();
        ByteBuf dataBuf = Unpooled.wrappedBuffer(array);

        if (dataBuf.capacity() == 0) return opaqueEntryList;

        int opaqueEntryListSize = CorfuProtocolCommon.fromBuffer(dataBuf, Integer.class);
        for (int i = 0; i < opaqueEntryListSize; i++) {
            opaqueEntryList.add(OpaqueEntry.deserialize(dataBuf));
        }

        return opaqueEntryList;
    }

    /**
     * Given a byte array, extract {@link OpaqueEntry}s.
     *
     * @param message  where to extract the data from
     * @return         list of extracted {@link OpaqueEntry}s
     */
    public static List<OpaqueEntry> extractOpaqueEntries(LogReplicationEntryMsg message) {
        return extractOpaqueEntries(message.getData().toByteArray());
    }

    /**
     * Given a list {@link OpaqueEntry}s, generate a byte array representation.
     *
     * @param opaqueEntryList list of {@link OpaqueEntry}s
     * @return                serialized representation
     */
    public static byte[] generatePayload(List<OpaqueEntry> opaqueEntryList) {
        ByteBuf buf = Unpooled.buffer();
        Integer size = opaqueEntryList.size();
        CorfuProtocolCommon.serialize(buf, size);

        for (OpaqueEntry opaqueEntry : opaqueEntryList) {
            OpaqueEntry.serialize(buf, opaqueEntry);
        }
        return buf.array();
    }

    /**
     * Given a sequence {@link OpaqueEntry}s, generate a byte array representation.
     *
     * @param opaqueEntries  sequence of {@link OpaqueEntry}s
     * @return               serialized representation
     */
    public static byte[] generatePayload(OpaqueEntry... opaqueEntries) {
        ByteBuf buf = Unpooled.buffer();
        Integer size = opaqueEntries.length;
        CorfuProtocolCommon.serialize(buf, size);

        for (OpaqueEntry opaqueEntry : opaqueEntries) {
            OpaqueEntry.serialize(buf, opaqueEntry);
        }
        return buf.array();
    }

    public static ResponseMsg getLeadershipResponse(
            HeaderMsg header, boolean isLeader, String nodeId, boolean isStandby) {
        LogReplication.LogReplicationLeadershipResponseMsg request = LogReplication.LogReplicationLeadershipResponseMsg
                .newBuilder()
                .setIsLeader(isLeader && isStandby)
                .setNodeId(nodeId).build();
        ResponsePayloadMsg payload = ResponsePayloadMsg.newBuilder()
                .setLrLeadershipResponse(request).build();
        return getResponseMsg(header, payload);
    }

    public static ResponseMsg getLeadershipLoss(HeaderMsg header, String nodeId) {
        LogReplication.LogReplicationLeadershipLossResponseMsg response = LogReplication.LogReplicationLeadershipLossResponseMsg
                .newBuilder()
                .setNodeId(nodeId)
                .build();
        ResponsePayloadMsg payload = ResponsePayloadMsg.newBuilder()
                .setLrLeadershipLoss(response).build();
        return getResponseMsg(header, payload);
    }
}
