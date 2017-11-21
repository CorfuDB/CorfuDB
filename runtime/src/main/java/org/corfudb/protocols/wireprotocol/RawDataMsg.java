package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Raw Log Entries map payload.
 */
@Data
@AllArgsConstructor
public class RawDataMsg implements ICorfuPayload<RawDataMsg> {

    private Map<Long, LogData> rawDataMap;

    public RawDataMsg(ByteBuf buf) {
        rawDataMap = ICorfuPayload.mapFromBuffer(buf, Long.class, LogData.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, rawDataMap);
    }
}
