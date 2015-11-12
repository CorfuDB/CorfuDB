package org.corfudb.infrastructure.wireprotocol;

import com.sun.corba.se.impl.orbutil.ObjectWriter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.json.*;
import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Created by dmalkhi on 11/9/15.
 */
@NoArgsConstructor
public class NettyLayoutServerRequestMsg extends NettyCorfuMsg {

    JsonObject jo = null;

    public NettyLayoutServerRequestMsg(JsonObject jsonObject)
    {
        this.msgType = NettyCorfuMsgType.LAYOUT_REQ;
        this.jo = jsonObject;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        JsonWriter jw = Json.createWriter(new ByteBufOutputStream(buffer));
        jw.writeObject(jo == null ? Json.createObjectBuilder().build() : jo);
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

        JsonReader jr = Json.createReader(new ByteBufInputStream(buffer));
        jo = jr.readObject();
    }
}
