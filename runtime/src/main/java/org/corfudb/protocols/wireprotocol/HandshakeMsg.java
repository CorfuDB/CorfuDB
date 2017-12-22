package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Message sent to initiate handshake between client and server.
 *
 * Created by amartinezman on 12/11/17.
 */
@CorfuPayload
@Data
@AllArgsConstructor
public class HandshakeMsg implements ICorfuPayload<HandshakeMsg> {
    private UUID clientId;
    private UUID serverId;

    /**
     * Constructor to generate an initiating Handshake Message Payload.
     *
     * @param buf The buffer to deserialize.
     */
    public HandshakeMsg(ByteBuf buf) {
        clientId = ICorfuPayload.fromBuffer(buf, UUID.class);
        serverId = ICorfuPayload.fromBuffer(buf, UUID.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, clientId);
        ICorfuPayload.serialize(buf, serverId);
    }
}
