package org.corfudb.protocols.wireprotocol.logreplication;

import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.runtime.Messages;

@Data
@Builder
public class LogReplicationNegotiationResponse implements ICorfuPayload<LogReplicationNegotiationResponse> {

    private long siteConfigID;
    private String version;
    private long snapshotStart;
    private long snapshotTransferred;
    private long snapshotApplied;
    private long lastLogProcessed;

    public LogReplicationNegotiationResponse(ByteBuf buf) {
        siteConfigID = ICorfuPayload.fromBuffer(buf, Long.class);
        version = ICorfuPayload.fromBuffer(buf, String.class);
        snapshotStart = ICorfuPayload.fromBuffer(buf, Long.class);
        snapshotTransferred = ICorfuPayload.fromBuffer(buf, Long.class);
        snapshotApplied = ICorfuPayload.fromBuffer(buf, Long.class);
        lastLogProcessed = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    public LogReplicationNegotiationResponse(long siteConfigID, String version, long snapshotStart, long lastTransferDone, long snapshotAppliedDone, long lastLogProcessed) {
        this.siteConfigID = siteConfigID;
        this.version = version;
        this.snapshotStart = snapshotStart;
        this.snapshotTransferred = lastTransferDone;
        this.snapshotApplied = snapshotAppliedDone;
        this.lastLogProcessed = lastLogProcessed;
    }

    public static LogReplicationNegotiationResponse fromProto(Messages.LogReplicationNegotiationResponse proto) {
        return new LogReplicationNegotiationResponse(proto.getSiteConfigID(),
                proto.getVersion(),
                proto.getSnapshotStart(),
                proto.getSnapshotTransferred(),
                proto.getSnapshotApplied(),
                proto.getLastLogEntryTimestamp());
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, siteConfigID);
        ICorfuPayload.serialize(buf, version);
        ICorfuPayload.serialize(buf, snapshotStart);
        ICorfuPayload.serialize(buf, snapshotTransferred);
        ICorfuPayload.serialize(buf, snapshotApplied);
        ICorfuPayload.serialize(buf, lastLogProcessed);
    }
}
