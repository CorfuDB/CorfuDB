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
public class NettyStreamingServerTokenRequestMsg extends NettyCorfuMsg {
    /** The streams to request tokens for */
    @Getter
    Set<UUID> streamIDs;

    /** The number of tokens to request */
    @Getter
    long numTokens;

        /* The wire format of the NettyStreamingServerTokenRequest message is below:
            | client ID(16) | request ID(8) |  type(1)  |   numStreams(1)  |stream ID(16)...| numTokens(8) |
            |  MSB  |  LSB  |               |           |                  |  MSB   |  LSB  |              |
            0       7       15              23          24                 25       32+     40+            48
         */

    public NettyStreamingServerTokenRequestMsg(Set<UUID> streamIDs, long numTokens)
    {
        this.msgType = NettyCorfuMsgType.TOKEN_REQ;
        this.numTokens = numTokens;
        this.streamIDs = streamIDs;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeByte((byte) streamIDs.size());
        for(UUID sid : streamIDs)
        {
            buffer.writeLong(sid.getMostSignificantBits());
            buffer.writeLong(sid.getLeastSignificantBits());
        }
        buffer.writeLong(numTokens);
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
        streamIDs = new HashSet<UUID>();
        byte numStreams = buffer.readByte();
        for (int i = 0; i < numStreams; i++)
        {
            streamIDs.add(new UUID(buffer.readLong(), buffer.readLong()));
        }
        numTokens = buffer.readLong();
    }
}
