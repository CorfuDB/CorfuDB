package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Request for known addresses in the log unit server for a specified range.
 * Created by zlokhandwala on 2019-06-01.
 */
@Data
@AllArgsConstructor
public class KnownAddressRequest implements ICorfuPayload<KnownAddressRequest> {

    private final Long startRange;
    private final Long endRange;

    /**
     * Deserialization Constructor from Bytebuf to KnownAddressRequest.
     *
     * @param buf The buffer to deserialize
     */
    public KnownAddressRequest(ByteBuf buf) {
        startRange = ICorfuPayload.fromBuffer(buf, Long.class);
        endRange = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, startRange);
        ICorfuPayload.serialize(buf, endRange);
    }
}
