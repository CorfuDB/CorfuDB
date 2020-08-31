package org.corfudb.protocols;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.proto.RpcCommon.LayoutMsg;
import org.corfudb.runtime.proto.RpcCommon.SequencerMetricsMsg;
import org.corfudb.runtime.proto.RpcCommon.SequencerMetricsMsg.SequencerStatus;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.view.Layout;

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

        public static final Map<Byte, MessageMarker> typeMap =
                Arrays.<MessageMarker>stream(MessageMarker.values())
                    .collect(Collectors.toMap(MessageMarker::asByte, Function.identity()));
    }

    public static final UUID DEFAULT_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    /**
     * Returns the Protobuf representation of a UUID.
     *
     * @param uuid   the desired Java UUID object
     * @return       an equivalent Protobuf UUID message
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
     * @param msg   the desired Protobuf UUID message
     * @return      an equivalent Java UUID object
     */
    public static UUID getUUID(UuidMsg msg) {
        return new UUID(msg.getMsb(), msg.getLsb());
    }

    /**
     * Returns the Protobuf representation of a Layout.
     *
     * @param layout   the desired Java Layout object
     * @return         an equivalent Protobuf Layout message
     */
    public static LayoutMsg getLayoutMsg(Layout layout) {
        if (layout == null) {
            return LayoutMsg.getDefaultInstance();
        }

        return LayoutMsg.newBuilder()
                .setLayoutJson(layout.asJSONString())
                .build();
    }

    /**
     * Returns a Layout object from its Protobuf representation.
     *
     * @param msg   the desired Protobuf Layout message
     * @return      an equivalent, potentially null, Java Layout object
     * @throws      SerializerException if unable to deserialize the JSON payload
     */
    public static Layout getLayout(LayoutMsg msg) {
        try {
            return Layout.fromJSONString(msg.getLayoutJson());
        } catch (NullPointerException npe) {
            return null;
        } catch (Exception ex) {
            throw new SerializerException("Unexpected error while deserializing Layout JSON", ex);
        }
    }

    /**
     * Returns the Protobuf representation of a Token, given
     * its epoch and sequence.
     *
     * @param epoch      the epoch of the token
     * @param sequence   the sequencer number of the token
     * @return           a TokenMsg containing the provided epoch and sequence values
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
     * @param token   the desired Java Token object
     * @return        an equivalent Protobuf Token message
     */
    public static TokenMsg getTokenMsg(Token token) {
        return getTokenMsg(token.getEpoch(), token.getSequence());
    }

    /**
     * Returns the Protobuf representation of a SequencerMetrics object.
     *
     * @param metrics   the desired Java SequencerMetrics object
     * @return          an equivalent Protobuf SequencerMetrics message
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
     * @param msg   the desired Protobuf SequencerMetrics message
     * @return      an equivalent Java SequencerMetrics object
     * @throws      UnsupportedOperationException if the status of the sequencer is unrecognized
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
}
