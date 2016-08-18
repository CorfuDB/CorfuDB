package org.corfudb.protocols.wireprotocol;

import com.google.common.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import lombok.*;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 9/15/15.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CorfuMsg {

    /**
     * Marker field value, should equal 0xC0FC0FC0
     */
    final static int markerField = 0xC0FC0FC0;
    static Map<Byte, CorfuMsgType> typeMap =
            Arrays.<CorfuMsgType>stream(CorfuMsgType.values())
                    .collect(Collectors.toMap(CorfuMsgType::asByte, Function.identity()));
    /**
     * The unique id of the client making the request
     */
    UUID clientID;
    /**
     * The request id of this request/response
     */
    long requestID;
    /**
     * The epoch of this request/response
     */
    long epoch;
    /**
     * The underlying ByteBuf, if present.
     */
    ByteBuf buf;

    ;
    /**
     * The type of message
     */
    CorfuMsgType msgType;

    /**
     * Constructor which generates a message based only the message type.
     * Typically used for generating error messages, since sendmessage will populate the rest of the fields.
     *
     * @param type The type of message to send.
     */
    public CorfuMsg(CorfuMsgType type) {
        this.msgType = type;
    }

        /* The wire format of the NettyCorfuMessage message is below:
        markerField(1) | client ID(8) | request ID(8) |  epoch(8)   |  type(1)  |
*/

    /**
     * Take the given bytebuffer and deserialize it into a message.
     *
     * @param buffer The buffer to deserialize.
     * @return The corresponding message.
     */
    public static CorfuMsg deserialize(ByteBuf buffer) {
        int marker = buffer.readInt();
        if (marker != markerField) {
            throw new RuntimeException("Attempt to deserialize a message which is not a CorfuMsg, "
                    + "Marker = " + marker + " but expected 0xC0FC0FC0");
        }
        UUID clientID = new UUID(buffer.readLong(), buffer.readLong());
        long requestID = buffer.readLong();
        long epoch = buffer.readLong();
        CorfuMsgType message = typeMap.get(buffer.readByte());
        CorfuMsg msg;
        try {
            msg = message.messageType
                    .constructor(message.messageType.getRawType().getConstructor()).invoke(null);
        } catch (NoSuchMethodException nsme) {
            throw new RuntimeException("Unrecognized message type " + message.toString());
        } catch (IllegalAccessException | InvocationTargetException ite) {
            throw new RuntimeException("Error deserializing message type " + message.toString());
        }
        msg.clientID = clientID;
        msg.requestID = requestID;
        msg.epoch = epoch;
        msg.msgType = message;
        msg.fromBuffer(buffer);
        msg.buf = buffer;
        return msg;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    public void serialize(ByteBuf buffer) {
        buffer.writeInt(markerField);
        if (clientID == null) {
            buffer.writeLong(0L);
            buffer.writeLong(0L);
        } else {
            buffer.writeLong(clientID.getMostSignificantBits());
            buffer.writeLong(clientID.getLeastSignificantBits());
        }
        buffer.writeLong(requestID);
        buffer.writeLong(epoch);
        buffer.writeByte(msgType.asByte());
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    public void fromBuffer(ByteBuf buffer) {
        // we don't do anything here since in the base message, no fields remain.
    }

    /**
     * Copy the base fields over to this message
     */
    public void copyBaseFields(CorfuMsg msg) {
        this.clientID = msg.clientID;
        this.epoch = msg.epoch;
        this.requestID = msg.requestID;
    }

    /**
     * Release the underlying buffer, if present.
     */
    public void release() {
        if (buf != null) {
            buf.release();
        }
    }

    @RequiredArgsConstructor
    @AllArgsConstructor
    public enum CorfuMsgType {
        // Base Messages
        PING(0, TypeToken.of(CorfuMsg.class), BaseServer.class, true),
        PONG(1, TypeToken.of(CorfuMsg.class), BaseServer.class, true),
        RESET(2, TypeToken.of(CorfuResetMsg.class), BaseServer.class,  true),
        SET_EPOCH(3, TypeToken.of(CorfuSetEpochMsg.class), LayoutServer.class, true),
        ACK(4, TypeToken.of(CorfuMsg.class), BaseServer.class, true),
        WRONG_EPOCH(5, TypeToken.of(CorfuSetEpochMsg.class), BaseServer.class),
        NACK(6, TypeToken.of(CorfuMsg.class), BaseServer.class),
        VERSION_REQUEST(7, TypeToken.of(CorfuMsg.class), BaseServer.class),
        VERSION_RESPONSE(8, new TypeToken<JSONPayloadMsg<VersionInfo>>(){}, BaseServer.class),

        // Layout Messages
        LAYOUT_REQUEST(10, TypeToken.of(CorfuMsg.class), LayoutServer.class, true),
        LAYOUT_RESPONSE(11, TypeToken.of(LayoutMsg.class), LayoutServer.class, true),
        LAYOUT_PREPARE(12, TypeToken.of(LayoutRankMsg.class), LayoutServer.class, true),
        LAYOUT_PREPARE_REJECT(13, TypeToken.of(LayoutRankMsg.class), LayoutServer.class),
        LAYOUT_PROPOSE(14, TypeToken.of(LayoutRankMsg.class), LayoutServer.class, true),
        LAYOUT_PROPOSE_REJECT(15, TypeToken.of(LayoutRankMsg.class), LayoutServer.class),
        LAYOUT_COMMITTED(16, TypeToken.of(LayoutRankMsg.class), LayoutServer.class, true),
        LAYOUT_QUERY(17, TypeToken.of(LayoutRankMsg.class), LayoutServer.class),
        LAYOUT_BOOTSTRAP(18, TypeToken.of(LayoutMsg.class), LayoutServer.class, true),
        LAYOUT_NOBOOTSTRAP(19, TypeToken.of(CorfuMsg.class), LayoutServer.class, true),

        // Sequencer Messages
        TOKEN_REQ(20, TypeToken.of(TokenRequestMsg.class), SequencerServer.class),
        TOKEN_RES(21, TypeToken.of(TokenResponseMsg.class), SequencerServer.class),

        // Logging Unit Messages
        WRITE(30, TypeToken.of(LogUnitWriteMsg.class), LogUnitServer.class),
        READ_REQUEST(31, TypeToken.of(LogUnitReadRequestMsg.class), LogUnitServer.class),
        READ_RESPONSE(32, TypeToken.of(LogUnitReadResponseMsg.class), LogUnitServer.class),
        TRIM(33, TypeToken.of(LogUnitTrimMsg.class), LogUnitServer.class),
        FILL_HOLE(34, TypeToken.of(LogUnitFillHoleMsg.class), LogUnitServer.class),
        FORCE_GC(35, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
        GC_INTERVAL(36, TypeToken.of(LogUnitGCIntervalMsg.class), LogUnitServer.class),
        FORCE_COMPACT(37, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
        READ_RANGE(40, TypeToken.of(CorfuRangeMsg.class), LogUnitServer.class),
        READ_RANGE_RESPONSE(41, TypeToken.of(LogUnitReadRangeResponseMsg.class), LogUnitServer.class),

        // Logging Unit Error Codes
        ERROR_OK(50, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
        ERROR_TRIMMED(51, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
        ERROR_OVERWRITE(52, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
        ERROR_OOS(53, TypeToken.of(CorfuMsg.class), LogUnitServer.class),
        ERROR_RANK(54, TypeToken.of(CorfuMsg.class), LogUnitServer.class),

        // EXTRA CODES
        LAYOUT_ALREADY_BOOTSTRAP(60, TypeToken.of(CorfuMsg.class), LayoutServer.class, true),
        LAYOUT_PREPARE_ACK(61, TypeToken.of(LayoutRankMsg.class), LayoutServer.class);


        public final int type;
        public final TypeToken<? extends CorfuMsg> messageType;
        public final Class<? extends AbstractServer> handler;
        public Boolean ignoreEpoch = false;

        public byte asByte() {
            return (byte) type;
        }
    }
}
