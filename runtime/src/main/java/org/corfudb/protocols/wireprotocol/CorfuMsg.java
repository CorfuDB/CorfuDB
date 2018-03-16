package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by mwei on 9/15/15.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CorfuMsg {

    /**
     * Marker field value, should equal 0xC0FC0FC0.
     */
    static final int markerField = 0xC0FC0FC0;
    static Map<Byte, CorfuMsgType> typeMap =
            Arrays.<CorfuMsgType>stream(CorfuMsgType.values())
                    .collect(Collectors.toMap(CorfuMsgType::asByte, Function.identity()));
    /**
     * The unique id of the client making the request.
     */
    @SuppressWarnings({"checkstyle:abbreviationaswordinname", "checkstyle:membername"})
    UUID clientID;

    /**
     * The request id of this request/response.
     */
    @SuppressWarnings({"checkstyle:abbreviationaswordinname", "checkstyle:membername"})
    long requestID;

    /**
     * The epoch of this request/response.
     */
    long epoch;

    /**
     * The underlying ByteBuf, if present.
     */
    ByteBuf buf;

    /**
     * The type of message.
     */
    CorfuMsgType msgType;

    /**
     * Constructor which generates a message based only the message type.
     * Typically used for generating error messages, since sendmessage
     * will populate the rest of the fields.
     *
     * @param type The type of message to send.
     */
    public CorfuMsg(CorfuMsgType type) {
        this.msgType = type;
    }

    // The wire format of the NettyCorfuMessage message is below:
    //    markerField(1) | client ID(8) | request ID(8) |  epoch(8)   |  type(1)  |

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
        UUID clientId = new UUID(buffer.readLong(), buffer.readLong());
        long requestId = buffer.readLong();
        long epoch = buffer.readLong();
        CorfuMsgType message = typeMap.get(buffer.readByte());
        CorfuMsg msg = message.getConstructor().construct();

        msg.clientID = clientId;
        msg.requestID = requestId;
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
     **/
    public void fromBuffer(ByteBuf buffer) {
        // we don't do anything here since in the base message, no fields remain.
    }

    /**
     * Copy the base fields over to this message.
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
}
