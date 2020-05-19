package org.corfudb.protocols.wireprotocol.logreplication;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.runtime.Messages;

@Data
public class LogReplicationQueryLeaderShipResponse implements ICorfuPayload<LogReplicationQueryLeaderShipResponse> {
    long epoch;
    boolean isLeader;

    public LogReplicationQueryLeaderShipResponse(long epoch, boolean isLeader) {
        this.epoch = epoch;
        this.isLeader = isLeader;
    }

    public LogReplicationQueryLeaderShipResponse(ByteBuf buf) {
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        isLeader = ICorfuPayload.fromBuffer(buf, Boolean.class);
    }

    public static LogReplicationQueryLeaderShipResponse fromProto(Messages.LogReplicationQueryLeadershipResponse proto) {
        return new LogReplicationQueryLeaderShipResponse(proto.getEpoch(), proto.getIsLeader());
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, isLeader);
    }
}
