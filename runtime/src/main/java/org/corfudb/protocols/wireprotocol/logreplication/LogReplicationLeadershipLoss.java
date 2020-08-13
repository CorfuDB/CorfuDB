package org.corfudb.protocols.wireprotocol.logreplication;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.runtime.Messages;

/**
 * This message is sent from the receiver to the sender whenever messages are
 * dropped due to loss of leadership or role change (no longer the receiver/standby).
 *
 * The loss of leadership will trigger leadership re-discovery on the sender.
 *
 * @author amartinezman
 */
public class LogReplicationLeadershipLoss implements ICorfuPayload<LogReplicationLeadershipLoss> {

    @Getter
    private final String endpoint;

    public LogReplicationLeadershipLoss(String endpoint) {
        this.endpoint = endpoint;
    }

    public LogReplicationLeadershipLoss(ByteBuf buf) {
        endpoint = ICorfuPayload.fromBuffer(buf, String.class);
    }

    public static LogReplicationLeadershipLoss fromProto(Messages.LogReplicationLeadershipLoss proto) {
        return new LogReplicationLeadershipLoss(proto.getEndpoint());
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, endpoint);
    }
}