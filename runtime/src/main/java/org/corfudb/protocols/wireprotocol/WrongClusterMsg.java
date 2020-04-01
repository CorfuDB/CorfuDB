package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.UUID;

/**
 * A message sent from the server router to the client in response to
 * the client sending an original message stamped with a clusterId that is different
 * from the server's clusterId.
 */
@Builder
@AllArgsConstructor
@Getter
public class WrongClusterMsg implements ICorfuPayload<WrongClusterMsg> {

    /**
     * Server's expected clusterId.
     */
    private final UUID serverClusterId;
    /**
     * ClusterId that belongs to the original client's message.
     */
    private final UUID clientClusterId;

    public WrongClusterMsg(ByteBuf buf){
        serverClusterId = ICorfuPayload.fromBuffer(buf, UUID.class);
        clientClusterId = ICorfuPayload.fromBuffer(buf, UUID.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, serverClusterId);
        ICorfuPayload.serialize(buf, clientClusterId);
    }
}
