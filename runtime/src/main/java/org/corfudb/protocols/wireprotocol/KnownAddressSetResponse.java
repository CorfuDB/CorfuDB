package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Response containing all the known addresses from a log unit server.
 */
@Data
@AllArgsConstructor
public class KnownAddressSetResponse implements ICorfuPayload<KnownAddressSetResponse> {

    private Set<Long> knownAddresses;


    public KnownAddressSetResponse(ByteBuf buf) {
        knownAddresses = ICorfuPayload.setFromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, knownAddresses);
    }
}
