package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.view.Address;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by WenbinZhu on 11/25/19.
 */
@AllArgsConstructor
public class LogRecoveryStateWriteMsg implements ICorfuPayload<LogRecoveryStateWriteMsg> {

    @Getter
    List<LogData> logEntries;

    @Getter
    List<LogData> garbageEntries;

    @Getter
    long compactionMark;

    public LogRecoveryStateWriteMsg(ByteBuf buf) {
        logEntries = ICorfuPayload.listFromBuffer(buf, LogData.class);
        garbageEntries = ICorfuPayload.listFromBuffer(buf, LogData.class);
        compactionMark = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    public LogRecoveryStateWriteMsg() {
        logEntries = new ArrayList<>();
        garbageEntries = new ArrayList<>();
        compactionMark = Address.NON_ADDRESS;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, logEntries);
        ICorfuPayload.serialize(buf, garbageEntries);
        ICorfuPayload.serialize(buf, compactionMark);
    }
}
