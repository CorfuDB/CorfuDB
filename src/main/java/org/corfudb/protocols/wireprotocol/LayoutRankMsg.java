package org.corfudb.protocols.wireprotocol;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.runtime.view.Layout;

import java.nio.charset.StandardCharsets;

/**
 * Created by mwei on 12/14/15.
 */
@ToString(callSuper = true)
public class LayoutRankMsg extends LayoutMsg {

    @Getter
    long rank;

    public LayoutRankMsg(Layout layout, long rank, CorfuMsgType type)
    {
        super(layout, type);
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
        buffer.writeLong(rank);
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        rank = buffer.readLong();
    }
}
