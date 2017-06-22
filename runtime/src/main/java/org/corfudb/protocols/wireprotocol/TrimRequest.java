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

    final UUID stream;
    final Long address;

    /**
     * Deserialization Constructor from Bytebuf to TrimRequest.
     *
     * @param buf The buffer to deserialize
     */
    public TrimRequest(ByteBuf buf) {
        if (ICorfuPayload.fromBuffer(buf, Boolean.class)) {
            stream = ICorfuPayload.fromBuffer(buf, UUID.class);
        } else {
            stream = null;
        }
        address = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, stream != null);
        if (stream != null) {
            ICorfuPayload.serialize(buf, stream);
        }
        ICorfuPayload.serialize(buf, address);
    }
}
