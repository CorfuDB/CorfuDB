package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.corfudb.runtime.view.Layout;

/**
 * Request in second phase of Paxos.
 * Created by mdhawan on 10/24/16.
 */
@Data
@AllArgsConstructor
public class LayoutProposeRequest implements ICorfuPayload<LayoutProposeRequest> {
    // Marks the boundary of a layout update.
    private long epoch;
    // Number that denotes a round started by a proposer (within an epoch).
    private long rank;
    // Value proposed
    private Layout layout;

    /**
     * Constructor for layout request in second phase of Paxos.
     */
    public LayoutProposeRequest(ByteBuf buf) {
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        rank = ICorfuPayload.fromBuffer(buf, Long.class);
        layout = ICorfuPayload.fromBuffer(buf, Layout.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, rank);
        ICorfuPayload.serialize(buf, layout);
    }
}
