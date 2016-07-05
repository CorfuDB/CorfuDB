package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.UUID;

/**
 * Created by mwei on 2/10/16.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class CorfuUUIDMsg extends CorfuMsg {

    UUID id;

    public CorfuUUIDMsg(UUID id) {
        this.id = id;
    }

    public CorfuUUIDMsg(CorfuMsgType msgType, UUID id) {
        this.msgType = msgType;
        this.id = id;
    }


    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        if (id == null) {
            buffer.writeByte(1);
        } else {
            buffer.writeByte(0);
            buffer.writeLong(id.getMostSignificantBits());
            buffer.writeLong(id.getLeastSignificantBits());
        }
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    @SuppressWarnings("unchecked")
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        if (buffer.readByte() == 1) {
            id = null;
        } else {
            id = new UUID(buffer.readLong(), buffer.readLong());
        }
    }
}
