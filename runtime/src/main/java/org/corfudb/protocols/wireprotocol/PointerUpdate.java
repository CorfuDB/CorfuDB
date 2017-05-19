package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.UUID;

/**
 * Created by jameskevin on 5/18/17.
 */
@CorfuPayload
@Data
public class PointerUpdate implements ICorfuPayload<PointerUpdate> {
    final UUID stream;
    final Long current;
    final Long next;

    public PointerUpdate(UUID stream, long current, long next) {
        this.stream = stream;
        this.current = new Long(current);
        this.next = new Long(next);
    }

    public PointerUpdate(ByteBuf buf) {
        stream = ICorfuPayload.fromBuffer(buf, UUID.class);
        current = ICorfuPayload.fromBuffer(buf, Long.class);
        next = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        if (stream != null) {
            ICorfuPayload.serialize(buf, stream);
            ICorfuPayload.serialize(buf, current);
            ICorfuPayload.serialize(buf, next);
        }
    }
}
