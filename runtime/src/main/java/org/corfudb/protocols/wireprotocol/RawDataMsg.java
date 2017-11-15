package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Raw Log Entries map payload.
 */
@Data
@AllArgsConstructor
public class RawDataMsg implements ICorfuPayload<RawDataMsg> {

    private List<LogData> entries;

    public RawDataMsg(ByteBuf buf) {
        entries = ICorfuPayload.listFromBuffer(buf, LogData.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, entries);
    }
}
