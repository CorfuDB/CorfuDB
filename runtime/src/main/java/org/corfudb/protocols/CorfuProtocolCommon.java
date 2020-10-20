package org.corfudb.protocols;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.proto.RpcCommon.LayoutMsg;
import org.corfudb.runtime.proto.RpcCommon.SequencerMetricsMsg;
import org.corfudb.runtime.proto.RpcCommon.SequencerMetricsMsg.SequencerStatus;
import org.corfudb.runtime.proto.RpcCommon.StreamAddressRangeMsg;
import org.corfudb.runtime.proto.RpcCommon.StreamAddressSpaceMsg;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidToStreamAddressSpacePairMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class provides methods for creating and converting between the Protobuf
 * objects defined in rpc_common.proto and their Java counterparts. These are used
 * by the majority of the service RPCs.
 */
@Slf4j
public class CorfuProtocolCommon {
    // Prevent class from being instantiated
    private CorfuProtocolCommon() {}

    private static final EnumMap<SequencerMetrics.SequencerStatus, SequencerStatus> sequencerStatusTypeMap =
            new EnumMap<>(ImmutableMap.of(
                    SequencerMetrics.SequencerStatus.READY, SequencerStatus.READY,
                    SequencerMetrics.SequencerStatus.NOT_READY, SequencerStatus.NOT_READY,
                    SequencerMetrics.SequencerStatus.UNKNOWN, SequencerStatus.UNKNOWN));

    // Temporary message header markers indicating message type.
    @AllArgsConstructor
    public enum MessageMarker {
        LEGACY_MSG_MARK(0x1),
        PROTO_REQUEST_MSG_MARK(0x2),
        PROTO_RESPONSE_MSG_MARK(0x3);

        private final int value;

        public byte asByte() {
            return (byte) value;
        }

        public static Map<Byte, MessageMarker> typeMap =
                Arrays.<MessageMarker>stream(MessageMarker.values())
                    .collect(Collectors.toMap(MessageMarker::asByte, Function.identity()));
    }

    public static final UUID DEFAULT_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    /**
     * Returns the Protobuf representation of a UUID.
     *
     * @param uuid   the desired (Java) UUID
     */
    public static UuidMsg getUuidMsg(UUID uuid) {
        return UuidMsg.newBuilder()
                .setLsb(uuid.getLeastSignificantBits())
                .setMsb(uuid.getMostSignificantBits())
                .build();
    }

    /**
     * Returns a UUID object from its Protobuf representation.
     *
     * @param msg   the Protobuf UUID message
     */
    public static UUID getUUID(UuidMsg msg) {
        return new UUID(msg.getMsb(), msg.getLsb());
    }

    /**
     * Returns the Protobuf representation of a Layout.
     *
     * @param layout   the desired (Java) Layout
     */
    public static LayoutMsg getLayoutMsg(Layout layout) {
        return LayoutMsg.newBuilder()
                .setLayoutJson(layout.asJSONString())
                .build();
    }

    /**
     * Returns a Layout object from its Protobuf representation.
     *
     * @param msg   the Protobuf Layout message
     */
    public static Layout getLayout(LayoutMsg msg) {
        return Layout.fromJSONString(msg.getLayoutJson());
    }

    /**
     * Returns the Protobuf representation of a Token, given
     * its epoch and sequence.
     *
     * @param epoch      the epoch of the token
     * @param sequence   the sequencer number of the token
     */
    public static TokenMsg getTokenMsg(long epoch, long sequence) {
        return TokenMsg.newBuilder()
                .setEpoch(epoch)
                .setSequence(sequence)
                .build();
    }

    /**
     * Returns the Protobuf representation of a Token.
     *
     * @param token   the desired (Java) Token
     */
    public static TokenMsg getTokenMsg(Token token) {
        return getTokenMsg(token.getEpoch(), token.getSequence());
    }

    /**
     * Returns the Protobuf representation of a SequencerMetrics object.
     *
     * @param metrics   the desired (Java) SequencerMetrics
     */
    public static SequencerMetricsMsg getSequencerMetricsMsg(SequencerMetrics metrics) {
        return SequencerMetricsMsg.newBuilder()
                .setSequencerStatus(sequencerStatusTypeMap.getOrDefault(
                        metrics.getSequencerStatus(), SequencerStatus.INVALID))
                .build();
    }

    /**
     * Returns a SequencerMetrics object from its Protobuf representation.
     *
     * @param msg   the Protobuf SequencerMetrics message
     */
    public static SequencerMetrics getSequencerMetrics(SequencerMetricsMsg msg) {
        switch (msg.getSequencerStatus()) {
            case READY:
                return SequencerMetrics.READY;
            case NOT_READY:
                return SequencerMetrics.NOT_READY;
            case UNKNOWN:
                return SequencerMetrics.UNKNOWN;
            default:
                throw new UnsupportedOperationException("SequencerMetrics message unrecognized: "
                        + "Status=" + msg.getSequencerStatus());
        }
    }

    /**
     * Returns the Protobuf representation of a StreamAddressSpace object.
     *
     * @param addressSpace   the desired (Java) StreamAddressSpace
     */
    public static StreamAddressSpaceMsg getStreamAddressSpaceMsg(StreamAddressSpace addressSpace) {
        StreamAddressSpaceMsg.Builder addressSpaceMsgBuilder = StreamAddressSpaceMsg.newBuilder();
        addressSpaceMsgBuilder.setTrimMark(addressSpace.getTrimMark());

        try (ByteString.Output bso = ByteString.newOutput()) {
            try (DataOutputStream dos = new DataOutputStream(bso)) {
                Roaring64NavigableMap rm = addressSpace.getAddressMap();
                // Improve compression
                rm.runOptimize();
                rm.serialize(dos);
                addressSpaceMsgBuilder.setAddressMap(bso.toByteString());
            }
        } catch (IOException ex) {
            throw new SerializerException("Unexpected error while serializing roaring64NavigableMap", ex);
        }

        return addressSpaceMsgBuilder.build();
    }

    /**
     * Returns a StreamAddressSpace object from its Protobuf representation.
     *
     * @param msg   the Protobuf StreamAddressSpace message
     */
    public static StreamAddressSpace getStreamAddressSpace(StreamAddressSpaceMsg msg) {
        Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();

        try (DataInputStream dis = new DataInputStream(msg.getAddressMap().newInput())) {
            roaring64NavigableMap.deserialize(dis);
        } catch (IOException ex) {
            throw new SerializerException("Unexpected error while deserializing roaring64NavigableMap", ex);
        }

        return new StreamAddressSpace(msg.getTrimMark(), roaring64NavigableMap);
    }

    /**
     * Returns the Protobuf representation of a StreamAddressRange object.
     *
     * @param streamAddressRange   the desired (Java) StreamAddressRange
     */
    public static StreamAddressRangeMsg getStreamAddressRangeMsg(StreamAddressRange streamAddressRange) {
        return StreamAddressRangeMsg.newBuilder()
                .setStreamId(getUuidMsg(streamAddressRange.getStreamID()))
                .setStart(streamAddressRange.getStart())
                .setEnd(streamAddressRange.getEnd())
                .build();
    }

    /**
     * Returns a StreamAddressResponse object from its log tail and List
     * of address map entries, each consisting of a UUID and a StreamAddressSpace,
     * represented in Protobuf.
     *
     * @param tail   the log tail
     * @param map    a list of address map entries represented in Protobuf
     */
    public static StreamsAddressResponse getStreamsAddressResponse(long tail, List<UuidToStreamAddressSpacePairMsg> map) {
        return new StreamsAddressResponse(tail, map.stream()
                .collect(Collectors.<UuidToStreamAddressSpacePairMsg, UUID, StreamAddressSpace>toMap(
                        entry -> getUUID(entry.getStreamUuid()),
                        entry -> getStreamAddressSpace(entry.getAddressSpace()))
                ));
    }
}
