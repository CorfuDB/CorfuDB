package org.corfudb.protocols.wireprotocol.logreplication;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.runtime.Messages;

@Data
public class LogReplicationMetadataResponse implements ICorfuPayload<LogReplicationMetadataResponse> {

    private final long topologyConfigId;
    private final String version;
    private final long snapshotStart;
    private final long snapshotTransferred;
    private final long snapshotApplied;
    private final long lastLogProcessed;

    public LogReplicationMetadataResponse(ByteBuf buf) {
        topologyConfigId = ICorfuPayload.fromBuffer(buf, Long.class);
        version = ICorfuPayload.fromBuffer(buf, String.class);
        snapshotStart = ICorfuPayload.fromBuffer(buf, Long.class);
        snapshotTransferred = ICorfuPayload.fromBuffer(buf, Long.class);
        snapshotApplied = ICorfuPayload.fromBuffer(buf, Long.class);
        lastLogProcessed = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    public LogReplicationMetadataResponse(long topologyConfigId, String version, long snapshotStartTimestamp,
                                          long snapshotTransferTimestamp, long snapshotAppliedTimestamp, long lastLogProcessed) {
        this.topologyConfigId = topologyConfigId;
        this.version = version;
        this.snapshotStart = snapshotStartTimestamp;
        this.snapshotTransferred = snapshotTransferTimestamp;
        this.snapshotApplied = snapshotAppliedTimestamp;
        this.lastLogProcessed = lastLogProcessed;
    }

    public static LogReplicationMetadataResponse fromProto(Messages.LogReplicationMetadataResponse proto) {
        return new LogReplicationMetadataResponse(proto.getSiteConfigID(),
                proto.getVersion(),
                proto.getSnapshotStart(),
                proto.getSnapshotTransferred(),
                proto.getSnapshotApplied(),
                proto.getLastLogEntryTimestamp());
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, topologyConfigId);
        ICorfuPayload.serialize(buf, version);
        ICorfuPayload.serialize(buf, snapshotStart);
        ICorfuPayload.serialize(buf, snapshotTransferred);
        ICorfuPayload.serialize(buf, snapshotApplied);
        ICorfuPayload.serialize(buf, lastLogProcessed);
    }
}
