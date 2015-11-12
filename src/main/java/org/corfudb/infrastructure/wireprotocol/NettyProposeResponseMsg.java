package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.NoArgsConstructor;

/**
 * Created by dalia on 11/11/15.
 */
@NoArgsConstructor
public class NettyProposeResponseMsg extends NettyCorfuMsg {

    boolean ack = false;

    public NettyProposeResponseMsg(boolean ack) {
        this.msgType = NettyCorfuMsg.NettyCorfuMsgType.META_PROPOSE_RES;
        this.ack = ack;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeBoolean(ack);
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
        ack = buffer.readBoolean();
    }
}
