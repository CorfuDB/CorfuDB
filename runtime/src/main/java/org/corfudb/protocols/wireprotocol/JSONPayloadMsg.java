package org.corfudb.protocols.wireprotocol;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.ParameterizedType;
import java.nio.charset.StandardCharsets;

import lombok.NoArgsConstructor;

/**
 * Created by mwei on 7/27/16.
 */
@NoArgsConstructor
@SuppressWarnings({"checkstyle:abbreviationaswordinname", "checkstyle:membername"})
public class JSONPayloadMsg<T> extends CorfuMsg {

    static final Gson parser = new GsonBuilder().create();

    /**
     * The payload.
     */
    private T payload;

    /**
     * The data, before deserialization.
     */
    private String dataString;

    public JSONPayloadMsg(T payload, CorfuMsgType type) {
        this.msgType = type;
        this.payload = payload;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        byte[] b = parser.toJson(payload).getBytes();
        buffer.writeInt(b.length);
        buffer.writeBytes(b);
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer to parse message from
     */
    @Override
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        int length = buffer.readInt();
        byte[] byteArray = new byte[length];
        buffer.readBytes(byteArray, 0, length);
        dataString = new String(byteArray, StandardCharsets.UTF_8);
    }

    /**
     * Get the payload. Uses some type reflection voodoo in order to get the type to the JSON
     * parser, so make sure to correctly parameterize this class, using with a raw type will
     * most definitely fail.
     *
     * @return  The JSON payload.
     */
    @SuppressWarnings("unchecked")
    public T getPayload() {
        return parser.fromJson(dataString,
                ((ParameterizedType)msgType.messageType.getType()).getActualTypeArguments()[0]);
    }
}
