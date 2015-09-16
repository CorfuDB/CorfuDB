package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Created by mwei on 9/15/15.
 */
@Getter
@Setter
@NoArgsConstructor
public class NettyStreamingServerTokenResponseMsg extends NettyCorfuMsg {
    /** The issued token */
    Long token;

        /* The wire format of the NettyStreamingServerTokenResponse message is below:
            | client ID(16) | request ID(8) |  type(1)  |  token(8) |
            |  MSB  |  LSB  |               |           |           |
            0       7       15              23          24          32
         */

    public NettyStreamingServerTokenResponseMsg(Long token)
    {
        this.msgType = NettyCorfuMsgType.TOKEN_RES;
        this.token = token;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeLong(this.token);
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
        this.token = buffer.readLong();
    }
}
