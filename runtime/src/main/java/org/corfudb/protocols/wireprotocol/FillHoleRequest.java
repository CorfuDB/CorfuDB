package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.UUID;

/**
 * Created by Maithem on 10/13/2016
 */
@CorfuPayload
@Data
@RequiredArgsConstructor
public class FillHoleRequest implements ICorfuPayload<FillHoleRequest> {

    final UUID stream;
    final Long prefix;

    public FillHoleRequest(ByteBuf buf) {
        if (ICorfuPayload.fromBuffer(buf, Boolean.class))
            stream = ICorfuPayload.fromBuffer(buf, UUID.class);
        else stream = null;
        prefix = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, stream != null);
        if (stream != null)
            ICorfuPayload.serialize(buf, stream);
        ICorfuPayload.serialize(buf, prefix);
    }
}