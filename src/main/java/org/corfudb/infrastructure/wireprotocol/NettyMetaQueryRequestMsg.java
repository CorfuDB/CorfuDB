package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import lombok.NoArgsConstructor;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;

/**
 * Created by dalia on 11/11/15.
 */
@NoArgsConstructor
public class NettyMetaQueryRequestMsg extends NettyCorfuMsg {

    int rank = -1;

    public NettyMetaQueryRequestMsg(NettyCorfuMsg.NettyCorfuMsgType t, int rank)
    {
        this.msgType = t;
        this.rank = rank;
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
    }
}
