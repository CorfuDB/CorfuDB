package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


/**
 * Created by mwei on 9/15/15.
 */
@Getter
@Setter
@NoArgsConstructor
public class NettyLogUnitGCIntervalMsg extends NettyCorfuMsg {


    /** The interval to use, in milliseconds */
    long interval;

    public NettyLogUnitGCIntervalMsg(long interval)
    {
        this.msgType = NettyCorfuMsgType.GC_INTERVAL;
        this.interval = interval;
    }
    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeLong(interval);
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
        interval = buffer.readLong();
    }
}
