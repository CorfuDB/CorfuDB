package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

/**
 * A request message to inspect a list of addresses,
 * which checks if any address in not committed.
 *
 * Created by WenbinZhu on 9/25/19.
 */
@Getter
@AllArgsConstructor
public class InspectAddressesRequest implements ICorfuPayload<InspectAddressesRequest> {

    // List of requested addresses to inspect.
    private final List<Long> addresses;

    /**
     * Deserialization Constructor from ByteBuf to InspectAddressesRequest.
     *
     * @param buf The buffer to deserialize
     */
    public InspectAddressesRequest(ByteBuf buf) {
        addresses = ICorfuPayload.listFromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
    }
}
