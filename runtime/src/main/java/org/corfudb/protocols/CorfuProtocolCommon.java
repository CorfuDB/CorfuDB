package org.corfudb.protocols;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.proto.Common.LayoutMsg;
import org.corfudb.runtime.proto.Common.SequencerMetricsMsg;
import org.corfudb.runtime.proto.Common.SequencerMetricsMsg.SequencerStatus;
import org.corfudb.runtime.proto.Common.StreamAddressRangeMsg;
import org.corfudb.runtime.proto.Common.StreamAddressSpaceMsg;
import org.corfudb.runtime.proto.Common.TokenMsg;
import org.corfudb.runtime.proto.Common.UuidMsg;
import org.corfudb.runtime.proto.Common.UuidToStreamAddressSpacePairMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class CorfuProtocolCommon {
    private static final EnumMap<SequencerMetrics.SequencerStatus, SequencerStatus> sequencerStatusTypeMap =
            new EnumMap<>(ImmutableMap.of(
                    SequencerMetrics.SequencerStatus.READY, SequencerStatus.READY,
                    SequencerMetrics.SequencerStatus.NOT_READY, SequencerStatus.NOT_READY,
                    SequencerMetrics.SequencerStatus.UNKNOWN, SequencerStatus.UNKNOWN));

    // Temporary message header markers indicating message type.
    public static final byte LEGACY_CORFU_MSG_MARK = 0x1;
    public static final byte PROTO_CORFU_REQUEST_MSG_MARK = 0x2;
    public static final byte PROTO_CORFU_RESPONSE_MSG_MARK = 0x3;

    public static UuidMsg getUuidMsg(UUID uuid) {
        return UuidMsg.newBuilder()
                .setLsb(uuid.getLeastSignificantBits())
                .setMsb(uuid.getMostSignificantBits())
                .build();
    }

    public static UUID getUUID(UuidMsg uuidMsg) {
        return new UUID(uuidMsg.getMsb(), uuidMsg.getLsb());
    }

    public static LayoutMsg getLayoutMsg(Layout layout) {
        return LayoutMsg.newBuilder()
                .setLayoutJson(layout.asJSONString())
                .build();
    }

    public static Layout getLayout(LayoutMsg layoutMsg) {
        return Layout.fromJSONString(layoutMsg.getLayoutJson());
    }

    public static TokenMsg getTokenMsg(long epoch, long sequence) {
        return TokenMsg.newBuilder()
                .setEpoch(epoch)
                .setSequence(sequence)
                .build();
    }

    public static TokenMsg getTokenMsg(Token token) {
        return getTokenMsg(token.getEpoch(), token.getSequence());
    }

    public static SequencerMetricsMsg getSequencerMetricsMsg(SequencerMetrics metrics) {
        return SequencerMetricsMsg.newBuilder()
                .setSequencerStatus(sequencerStatusTypeMap.get(metrics.getSequencerStatus()))
                .build();
    }

    public static SequencerMetrics getSequencerMetrics(SequencerMetricsMsg msg) {
        switch(msg.getSequencerStatus()) {
            case READY: return SequencerMetrics.READY;
            case NOT_READY: return SequencerMetrics.NOT_READY;
            case UNKNOWN: return SequencerMetrics.UNKNOWN;
            default:
        }

        //TODO(Zach): Return null or SequencerMetrics.UNKNOWN?
        return null;
    }

    public static StreamAddressSpaceMsg getStreamAddressSpaceMsg(StreamAddressSpace addressSpace) {
        StreamAddressSpaceMsg.Builder addressSpaceMsgBuilder = StreamAddressSpaceMsg.newBuilder();
        addressSpaceMsgBuilder.setTrimMark(addressSpace.getTrimMark());

        try(ByteString.Output bso = ByteString.newOutput()) {
            try(DataOutputStream dos = new DataOutputStream(bso)) {
                addressSpace.getAddressMap().serialize(dos);
                addressSpaceMsgBuilder.setAddressMap(bso.toByteString());
            }
        } catch (Exception ex) {
            log.error("getStreamAddressSpaceMsg: error=[{}, {}] " +
                    "while serializing roaring64NavigableMap", ex, ex.getCause());
        }

        return addressSpaceMsgBuilder.build();
    }

    public static StreamAddressSpace getStreamAddressSpace(StreamAddressSpaceMsg msg) {
        Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();

        try(DataInputStream dis = new DataInputStream(msg.getAddressMap().newInput())) {
            roaring64NavigableMap.deserialize(dis);
        } catch (Exception ex) {
            log.error("getStreamAddressSpace: error=[{}, {}] " +
                    "while deserializing roaring64NavigableMap", ex, ex.getCause());
        }

        return new StreamAddressSpace(msg.getTrimMark(), roaring64NavigableMap);
    }

    public static StreamAddressRangeMsg getStreamAddressRangeMsg(StreamAddressRange streamAddressRange) {
        return StreamAddressRangeMsg.newBuilder()
                .setStreamId(getUuidMsg(streamAddressRange.getStreamID()))
                .setStart(streamAddressRange.getStart())
                .setEnd(streamAddressRange.getEnd())
                .build();
    }

    public static StreamsAddressResponse getStreamsAddressResponse(long tail, List<UuidToStreamAddressSpacePairMsg> map) {
        return new StreamsAddressResponse(tail, map.stream()
                .collect(Collectors.<UuidToStreamAddressSpacePairMsg, UUID, StreamAddressSpace>toMap(
                        e -> getUUID(e.getStreamUuid()),
                        e -> getStreamAddressSpace(e.getAddressSpace()))
                ));
    }
}
