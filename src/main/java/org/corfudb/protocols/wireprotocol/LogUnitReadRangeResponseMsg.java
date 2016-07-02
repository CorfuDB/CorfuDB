package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mwei on 2/10/16.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class LogUnitReadRangeResponseMsg extends CorfuMsg {

    Map<Long, LogUnitReadResponseMsg> responseMap;

    public LogUnitReadRangeResponseMsg(Map<Long, LogUnitReadResponseMsg> map) {
        this.msgType = CorfuMsgType.READ_RANGE_RESPONSE;
        this.responseMap = map;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeInt(responseMap.size());
        for (Map.Entry<Long, LogUnitReadResponseMsg> e : responseMap.entrySet()) {
            buffer.writeLong(e.getKey());
            e.getValue().serialize(buffer);
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
        int size = buffer.readInt();
        responseMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            long address = buffer.readLong();
            LogUnitReadResponseMsg m = (LogUnitReadResponseMsg) (CorfuMsg.deserialize(buffer));
            responseMap.put(address, m);
        }
    }
}
