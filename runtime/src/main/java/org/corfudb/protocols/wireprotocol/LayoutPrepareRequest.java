package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Request in first phase of paxos.
 * Created by mdhawan on 10/24/16.
 */
@Data
@AllArgsConstructor
public class LayoutPrepareRequest implements ICorfuPayload<LayoutPrepareRequest> {
    // Marks the boundary of a layout update.
    private long epoch;
    // Number that denotes a round started by a proposer (within an epoch).
    private long rank;

    public LayoutPrepareRequest(ByteBuf buf) {
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        rank = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, rank);
    }
}