package org.corfudb.infrastructure.wireprotocol;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 9/10/15.
 */
@Data
public class NettyStreamingServerMsg {

    /** The unique id of the client making the request */
    final UUID clientID;

    /** The request id of this request/response */
    final long requestID;

    @RequiredArgsConstructor
    public enum NMStreamingServerMsgType {
        PING(0),
        PONG(1),
        RESET(2),
        TOKEN_REQ(20),
        TOKEN_RES(21)
        ;
        final int type;
        byte asByte() { return (byte)type; }
    };

    static Map<Byte, NMStreamingServerMsgType> typeMap =
                    Arrays.<NMStreamingServerMsgType>stream(NMStreamingServerMsgType.values())
                    .collect(Collectors.toMap(NMStreamingServerMsgType::asByte, Function.identity()));

    /** The type of message */
    final NMStreamingServerMsgType msgType;

    /** This class represents a token request */
    public static class NettyStreamingServerTokenRequest extends NettyStreamingServerMsg
    {
        /** The streams to request tokens for */
        @Getter
        final Set<UUID> streamIDs;

        /** The number of tokens to request */
        @Getter
        final long numTokens;

        public NettyStreamingServerTokenRequest(UUID clientID, long requestID, Set<UUID> streamIDs, long numTokens)
        {
            super(clientID, requestID, NMStreamingServerMsgType.TOKEN_REQ);
            this.streamIDs = streamIDs;
            this.numTokens = numTokens;
        }

        /* The wire format of the NettyStreamingServerTokenRequest message is below:
            | client ID(16) | request ID(8) |  type(1)  |   numStreams(1)  |stream ID(16)...| numTokens(8) |
            |  MSB  |  LSB  |               |           |                  |  MSB   |  LSB  |              |
            0       7       15              23          24                 25       32+     40+            48
         */

        public NettyStreamingServerTokenRequest(UUID clientID, long requestID, ByteBuf b)
        {
            super(clientID, requestID, NMStreamingServerMsgType.TOKEN_REQ);
            streamIDs = new HashSet<UUID>();
            byte numStreams = b.readByte();
            for (int i = 0; i < numStreams; i++)
            {
                streamIDs.add(new UUID(b.readLong(), b.readLong()));
            }
            numTokens = b.readLong();
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
    }

    /** This class represents a token response */
    public static class NettyStreamingServerTokenResponse extends NettyStreamingServerMsg
    {
        /** The issued token */
        @Getter
        final Long token;

        /* The wire format of the NettyStreamingServerTokenResponse message is below:
            | client ID(16) | request ID(8) |  type(1)  |  token(8) |
            |  MSB  |  LSB  |               |           |           |
            0       7       15              23          24          32
         */

        public NettyStreamingServerTokenResponse(UUID clientID, long requestID, Long token)
        {
            super(clientID, requestID, NMStreamingServerMsgType.TOKEN_RES);
            this.token = token;
        }

        public NettyStreamingServerTokenResponse(UUID clientID, long requestID, ByteBuf b)
        {
            super(clientID, requestID, NMStreamingServerMsgType.TOKEN_RES);
            this.token = b.readLong();
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
    }

    /* The wire format of the NMStreaming message is below:
        | client ID(16) | request ID(8) |  type(1)  |
        |  MSB  |  LSB  |               |           |
        0       7       15              23          24
     */
    /** Serialize the message into the given bytebuffer.
     * @param buffer    The buffer to serialize to.
     * */
    public void serialize(ByteBuf buffer) {
        buffer.writeLong(clientID.getMostSignificantBits());
        buffer.writeLong(clientID.getLeastSignificantBits());
        buffer.writeLong(requestID);
        buffer.writeByte(msgType.asByte());
    }

    /** Take the given bytebuffer and deserialize it into a message.
     *
     * @param buffer    The buffer to deserialize.
     * @return          The corresponding message.
     */
    public static NettyStreamingServerMsg deserialize(ByteBuf buffer) {
        UUID clientID = new UUID(buffer.readLong(), buffer.readLong());
        long requestID = buffer.readLong();
        NMStreamingServerMsgType message = typeMap.get(buffer.readByte());
        switch (message)
        {
            case TOKEN_REQ:
                return new NettyStreamingServerTokenRequest(clientID, requestID, buffer);
            case TOKEN_RES:
                return new NettyStreamingServerTokenResponse(clientID, requestID, buffer);
        }
        return new NettyStreamingServerMsg(clientID, requestID, message);
    }
}
