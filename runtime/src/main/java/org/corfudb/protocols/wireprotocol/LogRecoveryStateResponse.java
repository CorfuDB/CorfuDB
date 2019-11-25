package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.corfudb.runtime.view.Address;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by WenbinZhu on 11/25/19.
 */
@Data
@AllArgsConstructor
public class LogRecoveryStateResponse implements ICorfuPayload<LogRecoveryStateResponse> {

    private Map<Long, LogData> logEntryMap;

    private Map<Long, LogData> garbageEntryMap;

    long compactionMark;

    public LogRecoveryStateResponse(ByteBuf buf) {
        logEntryMap = ICorfuPayload.mapFromBuffer(buf, Long.class, LogData.class);
        garbageEntryMap = ICorfuPayload.mapFromBuffer(buf, Long.class, LogData.class);
        compactionMark = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    public LogRecoveryStateResponse() {
        logEntryMap = new HashMap<>();
        garbageEntryMap = new HashMap<>();
        compactionMark = Address.NON_ADDRESS;
    }

    public void putLogData(Long address, LogData data) {
        logEntryMap.put(address, data);
    }

    public void putGarbageData(Long address, LogData data) {
        garbageEntryMap.put(address, data);
    }

    public void removeGarbageData(Long address) {
        garbageEntryMap.remove(address);
    }

    public void merge(LogRecoveryStateResponse other) {
        logEntryMap.putAll(other.getLogEntryMap());
        garbageEntryMap.putAll(other.getGarbageEntryMap());
        compactionMark = Math.max(compactionMark, other.getCompactionMark());
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, logEntryMap);
        ICorfuPayload.serialize(buf, garbageEntryMap);
        ICorfuPayload.serialize(buf, compactionMark);
    }
}
