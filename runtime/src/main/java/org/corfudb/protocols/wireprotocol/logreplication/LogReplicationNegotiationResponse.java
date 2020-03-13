package org.corfudb.protocols.wireprotocol.logreplication;

import io.netty.buffer.ByteBuf;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

public class LogReplicationNegotiationResponse implements ICorfuPayload<LogReplicationNegotiationResponse> {

    private long baseSnapshotTimestamp;
    private long logEntryTimestamp;

    public LogReplicationNegotiationResponse(ByteBuf buf) {
        baseSnapshotTimestamp = ICorfuPayload.fromBuffer(buf, Long.class);
        logEntryTimestamp = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    public LogReplicationNegotiationResponse(long baseSnapshotTimestamp, long logEntryTimestamp) {
        this.baseSnapshotTimestamp = baseSnapshotTimestamp;
        this.logEntryTimestamp = logEntryTimestamp;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, baseSnapshotTimestamp);
        ICorfuPayload.serialize(buf, logEntryTimestamp);
    }
}
