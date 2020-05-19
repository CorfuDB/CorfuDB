package org.corfudb.protocols.wireprotocol.logreplication;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.runtime.Messages;

@Data
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

    public static LogReplicationNegotiationResponse fromProto(Messages.LogReplicationNegotiationResponse proto) {
        return new LogReplicationNegotiationResponse(proto.getBaseSnapshotTimestamp(), proto.getLogEntryTimestamp());
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, baseSnapshotTimestamp);
        ICorfuPayload.serialize(buf, logEntryTimestamp);
    }
}
