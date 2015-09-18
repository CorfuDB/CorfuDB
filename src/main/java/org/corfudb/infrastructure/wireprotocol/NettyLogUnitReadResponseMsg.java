package org.corfudb.infrastructure.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.corfudb.infrastructure.NettyLogUnitServer;

import java.util.EnumMap;


/**
 * Created by mwei on 9/15/15.
 */
@Getter
@Setter
@NoArgsConstructor
public class NettyLogUnitReadResponseMsg extends NettyLogUnitPayloadMsg {

    /** The result of this read. */
    NettyLogUnitServer.ReadResultType result;

    public NettyLogUnitReadResponseMsg(NettyLogUnitServer.ReadResultType result)
    {
        this.msgType = NettyCorfuMsgType.READ_RESPONSE;
        this.result = result;
    }

    public NettyLogUnitReadResponseMsg(NettyLogUnitServer.LogUnitEntry entry)
    {
        this.msgType = NettyCorfuMsgType.READ_RESPONSE;
        this.result = NettyLogUnitServer.ReadResultType.DATA;
        this.setMetadataMap(entry.getMetadataMap());
        this.setData(entry.getBuffer());
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeByte(result.asByte());
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
        result = NettyLogUnitServer.readResultTypeMap.get(buffer.readByte());
    }
}
