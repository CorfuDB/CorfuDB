package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.UUID;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Request sent to fill a hole.
 * Created by Maithem on 10/13/2016.
 */
@CorfuPayload
@Data
@RequiredArgsConstructor
public class FillHoleRequest implements ICorfuPayload<FillHoleRequest> {

    final Token address;

    /**
     * Constructor to generate a Fill Hole Request Payload.
     *
     * @param buf The buffer to deserialize.
     */
    public FillHoleRequest(ByteBuf buf) {
        long epoch = buf.readLong();
        long sequence = buf.readLong();
        address = new Token(epoch, sequence);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeLong(address.getEpoch());
        buf.writeLong(address.getSequence());
    }
}