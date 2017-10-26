package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Request to fetch all known addresses from a log unit server.
 */
@Data
@AllArgsConstructor
public class KnownAddressSetRequest implements ICorfuPayload<KnownAddressSetRequest> {

    private long startAddress;
    private long endAddress;


    public KnownAddressSetRequest(ByteBuf buf) {
        startAddress = ICorfuPayload.fromBuffer(buf, Long.class);
        endAddress = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, startAddress);
        ICorfuPayload.serialize(buf, endAddress);
    }
}
