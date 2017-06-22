package org.corfudb.protocols.wireprotocol;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import org.corfudb.runtime.view.Layout;

/**
 * Created by mwei on 12/14/15.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class LayoutMsg extends CorfuMsg {
    static final Gson parser = new GsonBuilder().create();
    /**
     * The current layout.
     */
    @Getter
    Layout layout;

    public LayoutMsg(Layout layout, CorfuMsgType type) {
        this.msgType = type;
        this.layout = layout;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        byte[] b = parser.toJson(layout).getBytes();
        buffer.writeInt(b.length);
        buffer.writeBytes(b);
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer The buffer to deserialize.
     */
    @Override
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        int length = buffer.readInt();
        byte[] byteArray = new byte[length];
        buffer.readBytes(byteArray, 0, length);
        String str = new String(byteArray, StandardCharsets.UTF_8);
        layout = parser.fromJson(str, Layout.class);
    }
}
