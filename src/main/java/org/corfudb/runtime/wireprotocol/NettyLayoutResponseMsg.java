package org.corfudb.runtime.wireprotocol;

/**
 * Created by mwei on 12/8/15.
 */

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.buffer.ByteBuf;
import lombok.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.Layout;

import java.nio.charset.StandardCharsets;

/**
 * Created by mwei on 9/15/15.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString
@Slf4j
public class NettyLayoutResponseMsg extends NettyCorfuMsg {


    /** The current layout. */
    Layout layout;

    static final Gson parser = new GsonBuilder().create();

    public NettyLayoutResponseMsg(Layout layout)
    {
        this.msgType = NettyCorfuMsgType.LAYOUT_RESPONSE;
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
        byte[] b =  parser.toJson(layout).getBytes();
        buffer.writeInt(b.length);
        buffer.writeBytes(b);
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend NettyCorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
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
