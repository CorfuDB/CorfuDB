package org.corfudb.protocols.wireprotocol;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

public class LogStatsRequest implements ICorfuPayload<LogStatsRequest> {
    @Getter
    long startAddress;
    @Getter
    long endAddress;

    public LogStatsRequest(long start, long end) {
        startAddress = start;
        endAddress = end;
    }

    public LogStatsRequest(ByteBuf buf) {
        this.startAddress = ICorfuPayload.fromBuffer(buf, Long.class);
        this.endAddress = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, this.startAddress);
        ICorfuPayload.serialize(buf, this.endAddress);
    }
}

