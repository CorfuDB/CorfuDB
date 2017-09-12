package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Request to add a new node.
 */
@Data
@AllArgsConstructor
public class AddNodeRequest implements ICorfuPayload<AddNodeRequest> {

    private String endpoint;
    private boolean isLayoutServer;
    private boolean isSequencerServer;
    private boolean isLogUnitServer;
    private boolean isUnresponsiveServer;
    private int logUnitStripeIndex;

    public AddNodeRequest(ByteBuf buf) {
        endpoint = ICorfuPayload.fromBuffer(buf, String.class);
        isLayoutServer = ICorfuPayload.fromBuffer(buf, Boolean.class);
        isSequencerServer = ICorfuPayload.fromBuffer(buf, Boolean.class);
        isLogUnitServer = ICorfuPayload.fromBuffer(buf, Boolean.class);
        isUnresponsiveServer = ICorfuPayload.fromBuffer(buf, Boolean.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, endpoint);
        ICorfuPayload.serialize(buf, isLayoutServer);
        ICorfuPayload.serialize(buf, isSequencerServer);
        ICorfuPayload.serialize(buf, isLogUnitServer);
        ICorfuPayload.serialize(buf, isUnresponsiveServer);
    }
}
