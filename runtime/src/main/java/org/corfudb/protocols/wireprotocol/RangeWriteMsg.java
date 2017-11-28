package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 *
 * A sequence of log entries to write
 *
 * @author Maithem
 */
@Data
@AllArgsConstructor
public class RangeWriteMsg implements ICorfuPayload<RangeWriteMsg> {

    private List<LogData> entries;

    public RangeWriteMsg(ByteBuf buf) {
        entries = ICorfuPayload.listFromBuffer(buf, LogData.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, entries);
    }
}
