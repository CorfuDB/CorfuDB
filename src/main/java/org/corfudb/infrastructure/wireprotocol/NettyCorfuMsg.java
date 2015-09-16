package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;

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
public class NettyCorfuMsg {
    /** The unique id of the client making the request */
    UUID clientID;

    /** The request id of this request/response */
    long requestID;

    /** The epoch of this request/response */
    long epoch;

    @RequiredArgsConstructor
    public enum NettyCorfuMsgType {
        PING(0, NettyCorfuMsg.class),
        PONG(1, NettyCorfuMsg.class),
        RESET(2, NettyCorfuResetMsg.class),
        SET_EPOCH(3, NettyCorfuSetEpochMsg.class),
        ACK(4, NettyCorfuMsg.class),
        WRONG_EPOCH(5, NettyCorfuMsg.class),
        TOKEN_REQ(20, NettyStreamingServerTokenRequestMsg.class),
        TOKEN_RES(21, NettyStreamingServerTokenResponseMsg.class)
        ;

        final int type;
        final Class<? extends NettyCorfuMsg> messageType;

        byte asByte() { return (byte)type; }
    };

    static Map<Byte, NettyCorfuMsgType> typeMap =
            Arrays.<NettyCorfuMsgType>stream(NettyCorfuMsgType.values())
                    .collect(Collectors.toMap(NettyCorfuMsgType::asByte, Function.identity()));

    /** The type of message */
    NettyCorfuMsgType msgType;

        /* The wire format of the NettyCorfuMessage message is below:
        | client ID(16) | request ID(8) |  epoch(8)   |  type(1)  |
        |  MSB  |  LSB  |               |             |           |
        0       7       15              23            31          32
*/
    /** Serialize the message into the given bytebuffer.
     * @param buffer    The buffer to serialize to.
     * */
    public void serialize(ByteBuf buffer) {
        buffer.writeLong(clientID.getMostSignificantBits());
        buffer.writeLong(clientID.getLeastSignificantBits());
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
        UUID clientID = new UUID(buffer.readLong(), buffer.readLong());
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
}
