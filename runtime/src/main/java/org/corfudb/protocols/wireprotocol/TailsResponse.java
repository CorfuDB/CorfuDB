package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import org.corfudb.runtime.view.Layout;

/**
 *
 * A container that contains information about the log's stream tails
 * map and max global address written (i.e. global tail)
 *
 * Created by Maithem on 10/22/18.
 */

@Data
@RequiredArgsConstructor
@AllArgsConstructor
public class TailsResponse implements ICorfuPayload<TailsResponse> {

    long epoch = Layout.INVALID_EPOCH;

    final long logTail;

    final Map<UUID, Long> streamTails;

    public TailsResponse(long logTail) {
        this.logTail = logTail;
        streamTails = Collections.EMPTY_MAP;
    }

    public TailsResponse(ByteBuf buf) {
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        logTail = ICorfuPayload.fromBuffer(buf, Long.class);
        streamTails = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, logTail);
        ICorfuPayload.serialize(buf, streamTails);
    }
}
