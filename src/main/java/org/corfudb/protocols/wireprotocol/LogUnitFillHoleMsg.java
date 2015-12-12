package org.corfudb.protocols.wireprotocol;

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
public class LogUnitFillHoleMsg extends CorfuMsg {


    /** The address to fill the hole at. */
    long address;

    public LogUnitFillHoleMsg(long address)
    {
        this.msgType = NettyCorfuMsgType.FILL_HOLE;
        this.address = address;
    }
    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeLong(address);
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
        address = buffer.readLong();
    }
}
