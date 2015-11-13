package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import java.io.BufferedReader;
import java.io.StringReader;

/**
 * Created by dmalkhi on 11/9/15.
 */
@Getter
@Setter
@NoArgsConstructor
public class NettyLayoutConfigMsg extends NettyCorfuMsg {

    JsonObject jo = null;
    int rank = -1;

    public NettyLayoutConfigMsg(NettyCorfuMsg.NettyCorfuMsgType t, int rank, JsonObject jo)
    {
        this.jo = jo;
        this.msgType = t;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeInt(rank);
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

        rank = buffer.readInt();
        JsonReader jr = Json.createReader(new ByteBufInputStream(buffer));
        jo = jr.readObject();
    }
}
