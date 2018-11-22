package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.UUID;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Created by mwei on 8/8/16.
 */
@CorfuPayload
@Data
@RequiredArgsConstructor
public class TrimRequest implements ICorfuPayload<TrimRequest> {

    final Token address;

    /**
     * Deserialization Constructor from Bytebuf to TrimRequest.
     *
     * @param buf The buffer to deserialize
     */
    public TrimRequest(ByteBuf buf) {
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
