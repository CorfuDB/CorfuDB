package org.corfudb.protocols.wireprotocol.logreplication;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.runtime.Messages.LogReplicationQueryLeadershipResponse;

@Data
public class LogReplicationQueryLeaderShipResponse implements ICorfuPayload<LogReplicationQueryLeaderShipResponse> {

    private final long epoch;
    private final boolean isLeader;
    private final String nodeId;

    public LogReplicationQueryLeaderShipResponse(long epoch, boolean isLeader, String nodeId) {
        this.epoch = epoch;
        this.isLeader = isLeader;
        this.nodeId = nodeId;
    }

    public LogReplicationQueryLeaderShipResponse(ByteBuf buf) {
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        isLeader = ICorfuPayload.fromBuffer(buf, Boolean.class);
        nodeId = ICorfuPayload.fromBuffer(buf, String.class);
    }

    public static LogReplicationQueryLeaderShipResponse fromProto(LogReplicationQueryLeadershipResponse proto) {
        return new LogReplicationQueryLeaderShipResponse(proto.getEpoch(), proto.getIsLeader(), proto.getNodeId());
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, isLeader);
        ICorfuPayload.serialize(buf, nodeId);
    }
}
