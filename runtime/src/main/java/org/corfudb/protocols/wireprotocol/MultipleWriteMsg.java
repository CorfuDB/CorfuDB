package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 *
 * A list of log entries to write
 *
 * @author Maithem
 */
@Data
@AllArgsConstructor
public class MultipleWriteMsg implements ICorfuPayload<MultipleWriteMsg> {

    private List<LogData> entries;

    public MultipleWriteMsg(ByteBuf buf) {
        entries = ICorfuPayload.listFromBuffer(buf, LogData.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, entries);
    }
}
