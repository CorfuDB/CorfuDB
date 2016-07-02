package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Created by mwei on 9/15/15.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class TokenRequestMsg extends CorfuMsg {
    /**
     * The streams to request tokens for
     */
    @Getter
    Set<UUID> streamIDs;

    /**
     * The number of tokens to request
     */
    @Getter
    long numTokens;

    /**
     * Any flags to set for the token request.
     */
    @Getter
    Set<TokenRequestFlags> tokenFlags;

    public TokenRequestMsg(Set<UUID> streamIDs, long numTokens) {
        this.msgType = CorfuMsgType.TOKEN_REQ;
        this.numTokens = numTokens;
        this.streamIDs = streamIDs;
        this.tokenFlags = EnumSet.noneOf(TokenRequestFlags.class);
    }
        /* The wire format of the NettyStreamingServerTokenRequest message is below:
            | client ID(16) | request ID(8) |  type(1)  |   numStreams(1)  |stream ID(16)...| numTokens(8) |
            |  MSB  |  LSB  |               |           |                  |  MSB   |  LSB  |              |
            0       7       15              23          24                 25       32+     40+            48
         */

    public TokenRequestMsg(Set<UUID> streamIDs, long numTokens, Set<TokenRequestFlags> tokenFlags) {
        this.msgType = CorfuMsgType.TOKEN_REQ;
        this.numTokens = numTokens;
        this.streamIDs = streamIDs;
        this.tokenFlags = tokenFlags;
    }

    public static Set<TokenRequestFlags> flagsFromShort(short flagsShort) {
        Set<TokenRequestFlags> flagsSet = EnumSet.noneOf(TokenRequestFlags.class);
        for (TokenRequestFlags flag : TokenRequestFlags.values()) {
            if ((flagsShort & flag.flag) == flag.flag) {
                flagsSet.add(flag);
            }
        }
        return flagsSet;
    }

    public static short shortFromFlags(Set<TokenRequestFlags> flags) {
        short retVal = 0;
        for (TokenRequestFlags flag : flags) {
            retVal = (short) (retVal | flag.flag);
        }
        return retVal;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeShort(shortFromFlags(tokenFlags));
        buffer.writeByte((byte) streamIDs.size());
        for (UUID sid : streamIDs) {
            buffer.writeLong(sid.getMostSignificantBits());
            buffer.writeLong(sid.getLeastSignificantBits());
        }
        buffer.writeLong(numTokens);
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
        tokenFlags = flagsFromShort(buffer.readShort());
        streamIDs = new HashSet<UUID>();
        byte numStreams = buffer.readByte();
        for (int i = 0; i < numStreams; i++) {
            streamIDs.add(new UUID(buffer.readLong(), buffer.readLong()));
        }
        numTokens = buffer.readLong();
    }

    public enum TokenRequestFlags {
        STREAM_HINT((short) 1);

        private final short flag;

        TokenRequestFlags(short flag) {
            this.flag = flag;
        }
    }
}
