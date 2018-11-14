package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 *
 * A container that contains information about the log's stream tails
 * map and max global address written (i.e. global tail)
 *
 * Created by Maithem on 10/22/18.
 */

@Data
@RequiredArgsConstructor
public class TailsResponse implements ICorfuPayload<TailsResponse> {

    final Token tail;

    final Map<UUID, Token> streamTails;

    public TailsResponse(ByteBuf buf) {
        tail = ICorfuPayload.fromBuffer(buf, Long.class);
        streamTails = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, logTail);
        ICorfuPayload.serialize(buf, streamTails);
    }
}
