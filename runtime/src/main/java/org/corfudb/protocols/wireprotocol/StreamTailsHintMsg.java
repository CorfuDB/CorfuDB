package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;
import java.util.UUID;

/**
 * Created by rmichoud on 6/20/17.
 */
@Data
@AllArgsConstructor
public class StreamTailsHintMsg implements ICorfuPayload<StreamTailsHintMsg> {
    private Map<UUID, Long> streamTailHint;

    public StreamTailsHintMsg(ByteBuf buf) {
        streamTailHint = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, streamTailHint);
    }
}

