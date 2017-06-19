package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * {@link org.corfudb.infrastructure.LayoutServer} response in second phase of paxos.
 * {@link org.corfudb.infrastructure.LayoutServer} will reject the proposal
 * if the last accepted prepare was not for this propose.
 *
 * <p>Created by mdhawan on 10/24/16.</p>
 */
@Data
@AllArgsConstructor
public class LayoutProposeResponse implements ICorfuPayload<LayoutProposeResponse> {
    private long rank;

    public LayoutProposeResponse(ByteBuf buf) {
        rank = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, rank);
    }
}