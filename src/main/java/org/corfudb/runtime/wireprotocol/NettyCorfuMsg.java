package org.corfudb.runtime.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;
import org.corfudb.infrastructure.*;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 9/15/15.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class NettyCorfuMsg {
    /** The unique id of the client making the request */
    long clientID;

    /** The request id of this request/response */
    long requestID;

    /** The epoch of this request/response */
    long epoch;

    @RequiredArgsConstructor
    public enum NettyCorfuMsgType {
        // Base Messages
        PING(0, NettyCorfuMsg.class, BaseNettyServer.class),
        PONG(1, NettyCorfuMsg.class, BaseNettyServer.class),
        RESET(2, NettyCorfuResetMsg.class, BaseNettyServer.class),
        SET_EPOCH(3, NettyCorfuSetEpochMsg.class, BaseNettyServer.class),
        ACK(4, NettyCorfuMsg.class, BaseNettyServer.class),
        WRONG_EPOCH(5, NettyCorfuMsg.class, BaseNettyServer.class),

        // Layout Messages
        LAYOUT_REQUEST(10, NettyCorfuMsg.class, LayoutServer.class),
        LAYOUT_RESPONSE(11, NettyLayoutResponseMsg.class, LayoutServer.class),

        // Sequencer Messages
        TOKEN_REQ(20, NettyStreamingServerTokenRequestMsg.class, SequencerServer.class),
        TOKEN_RES(21, NettyStreamingServerTokenResponseMsg.class, SequencerServer.class),

        // Logging Unit Messages
        WRITE(30, NettyLogUnitWriteMsg.class, LogUnitServer.class),
        READ_REQUEST(31, NettyLogUnitReadRequestMsg.class, LogUnitServer.class),
        READ_RESPONSE(32, NettyLogUnitReadResponseMsg.class, LogUnitServer.class),
        TRIM(33, NettyLogUnitTrimMsg.class, LogUnitServer.class),
        FILL_HOLE(34, NettyLogUnitFillHoleMsg.class, LogUnitServer.class),
        FORCE_GC(35, NettyCorfuMsg.class, LogUnitServer.class),
        GC_INTERVAL(36, NettyLogUnitGCIntervalMsg.class, LogUnitServer.class),

        // Logging Unit Error Codes
        ERROR_OK(40, NettyCorfuMsg.class, LogUnitServer.class),
        ERROR_TRIMMED(41, NettyCorfuMsg.class, LogUnitServer.class),
        ERROR_OVERWRITE(42, NettyCorfuMsg.class, LogUnitServer.class),
        ERROR_OOS(43, NettyCorfuMsg.class, LogUnitServer.class),
        ERROR_RANK(44, NettyCorfuMsg.class, LogUnitServer.class)
        ;

        public final int type;
        public final Class<? extends NettyCorfuMsg> messageType;
        public final Class<? extends INettyServer> handler;

        public byte asByte() { return (byte)type; }
    };

    static Map<Byte, NettyCorfuMsgType> typeMap =
            Arrays.<NettyCorfuMsgType>stream(NettyCorfuMsgType.values())
                    .collect(Collectors.toMap(NettyCorfuMsgType::asByte, Function.identity()));

    /** The type of message */
    NettyCorfuMsgType msgType;

        /* The wire format of the NettyCorfuMessage message is below:
        | client ID(8) | request ID(8) |  epoch(8)   |  type(1)  |
*/
    /** Serialize the message into the given bytebuffer.
     * @param buffer    The buffer to serialize to.
     * */
    public void serialize(ByteBuf buffer) {
        buffer.writeLong(clientID);
        buffer.writeLong(requestID);
        buffer.writeLong(epoch);
        buffer.writeByte(msgType.asByte());
    }

    /** Parse the rest of the message from the buffer. Classes that extend NettyCorfuMsg
     * should parse their fields in this method.
     * @param buffer
     */
    public void fromBuffer(ByteBuf buffer) {
        // we don't do anything here since in the base message, no fields remain.
    }

    /** Copy the base fields over to this message */
    public void copyBaseFields(NettyCorfuMsg msg)
    {
        this.clientID = msg.clientID;
        this.epoch = msg.epoch;
        this.requestID = msg.requestID;
    }

    /** Take the given bytebuffer and deserialize it into a message.
     *
     * @param buffer    The buffer to deserialize.
     * @return          The corresponding message.
     */
    @SneakyThrows
    public static NettyCorfuMsg deserialize(ByteBuf buffer) {
        long clientID = buffer.readLong();
        long requestID = buffer.readLong();
        long epoch = buffer.readLong();
        NettyCorfuMsgType message = typeMap.get(buffer.readByte());
        NettyCorfuMsg msg = message.messageType.getConstructor().newInstance();
        msg.clientID = clientID;
        msg.requestID = requestID;
        msg.epoch = epoch;
        msg.msgType = message;
        msg.fromBuffer(buffer);
        return msg;
    }

    /** Constructor which generates a message based only the message type.
     * Typically used for generating error messages, since sendmessage will populate the rest of the fields.
     * @param type  The type of message to send.
     */
    public NettyCorfuMsg(NettyCorfuMsgType type)
    {
        this.msgType = type;
    }
}
