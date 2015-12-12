package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.*;


/**
 * Created by mwei on 9/15/15.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString
public class LogUnitWriteMsg extends LogUnitPayloadMsg {


    /** The address to write to */
    long address;

    public LogUnitWriteMsg(long address)
    {
        this.msgType = NettyCorfuMsgType.WRITE;
        this.address = address;
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    }


    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    @SuppressWarnings("unchecked")
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
