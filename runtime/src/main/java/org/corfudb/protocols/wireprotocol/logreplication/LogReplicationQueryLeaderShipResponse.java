package org.corfudb.protocols.wireprotocol.logreplication;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.runtime.Messages.LogReplicationQueryLeadershipResponse;

@Data
public class LogReplicationQueryLeaderShipResponse implements ICorfuPayload<LogReplicationQueryLeaderShipResponse> {

    private final long epoch;
    private final boolean isLeader;
    private final String endpoint;

    public LogReplicationQueryLeaderShipResponse(long epoch, boolean isLeader, String endpoint) {
        this.epoch = epoch;
        this.isLeader = isLeader;
        this.endpoint = endpoint;
    }

    public LogReplicationQueryLeaderShipResponse(ByteBuf buf) {
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        isLeader = ICorfuPayload.fromBuffer(buf, Boolean.class);
        endpoint = ICorfuPayload.fromBuffer(buf, String.class);
    }

    public static LogReplicationQueryLeaderShipResponse fromProto(LogReplicationQueryLeadershipResponse proto) {
        return new LogReplicationQueryLeaderShipResponse(proto.getEpoch(), proto.getIsLeader(), proto.getEndpoint());
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, isLeader);
        ICorfuPayload.serialize(buf, endpoint);
    }
}
