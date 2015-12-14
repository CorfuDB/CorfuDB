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
public class CorfuSetEpochMsg extends CorfuMsg {


    /** The new epoch to move to. */
    long newEpoch;

    public CorfuSetEpochMsg(long newEpoch)
    {
        this.msgType = CorfuMsgType.SET_EPOCH;
        this.newEpoch = newEpoch;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeLong(newEpoch);
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
        newEpoch = buffer.readLong();
    }
}
