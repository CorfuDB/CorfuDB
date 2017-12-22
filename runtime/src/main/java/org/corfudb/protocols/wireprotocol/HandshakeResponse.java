package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Response sent by server in the handshake stage.
 *
 * Created by amartinezman on 12/11/17.
 */
@CorfuPayload
@Data
@AllArgsConstructor
public class HandshakeResponse implements ICorfuPayload<HandshakeResponse> {
    private UUID serverId;
    private String corfuVersion;

    public HandshakeResponse(ByteBuf buf) {
        serverId = ICorfuPayload.fromBuffer(buf, UUID.class);
        corfuVersion = ICorfuPayload.fromBuffer(buf, String.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, serverId);
        ICorfuPayload.serialize(buf, corfuVersion);
    }
}
