package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

import org.corfudb.runtime.view.Layout;

/**
 * {@link org.corfudb.infrastructure.LayoutServer} response in phase1 of paxos
 * can be an accept or reject.
 * If a {@link org.corfudb.infrastructure.LayoutServer} accepts (the proposal rank is higher than
 * any seen by the server so far), it will send back a previously agreed {@link Layout}.
 * {@link org.corfudb.infrastructure.LayoutServer} will reject any proposal with a rank
 * less than or equal to any already seen by the server.
 *
 * <p>Created by mdhawan on 10/24/16.</p>
 */
@Data
@AllArgsConstructor
public class LayoutPrepareResponse implements ICorfuPayload<LayoutPrepareResponse> {
    private long rank;
    private Layout layout;

    /**
     * Constructor for layout server response in first phase of Paxos.
     */
    public LayoutPrepareResponse(ByteBuf buf) {
        rank = ICorfuPayload.fromBuffer(buf, Long.class);
        boolean layoutPresent = ICorfuPayload.fromBuffer(buf, Boolean.class);
        if (layoutPresent) {
            layout = ICorfuPayload.fromBuffer(buf, Layout.class);
        }
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, rank);
        boolean layoutPresent = layout != null;
        ICorfuPayload.serialize(buf, layoutPresent);
        if (layoutPresent) {
            ICorfuPayload.serialize(buf, layout);
        }
    }


}