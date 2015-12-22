package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.UUID;


/**
 * Created by mwei on 9/15/15.
 */
@Getter
@Setter
@NoArgsConstructor
public class LogUnitTrimMsg extends CorfuMsg {


    /** The address to prefix trim, inclusive. */
    long prefix;

    /** The stream ID to trim. */
    UUID streamID;

    public LogUnitTrimMsg(long prefix, UUID streamID)
    {
        this.msgType = CorfuMsgType.TRIM;
        this.streamID = streamID;
        this.prefix = prefix;
    }
    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeLong(prefix);
        buffer.writeLong(streamID.getMostSignificantBits());
        buffer.writeLong(streamID.getMostSignificantBits());
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
        prefix = buffer.readLong();
        streamID = new UUID(buffer.readLong(), buffer.readLong());
    }
}
