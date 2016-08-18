package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.util.UUID;

/**
 * Created by mwei on 8/8/16.
 */
@CorfuPayload
@Data
@RequiredArgsConstructor
public class TrimRequest implements ICorfuPayload<TrimRequest> {

    final UUID stream;
    final Long prefix;

    public TrimRequest(ByteBuf buf) {
        stream = ICorfuPayload.fromBuffer(buf, UUID.class);
        prefix = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, stream);
        ICorfuPayload.serialize(buf, prefix);
    }
}
