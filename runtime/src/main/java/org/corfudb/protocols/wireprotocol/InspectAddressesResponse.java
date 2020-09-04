package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * A response message containing a list of uncommitted addresses.
 *
 * Created by WenbinZhu on 5/4/20.
 */
@AllArgsConstructor
public class InspectAddressesResponse implements ICorfuPayload<InspectAddressesResponse> {

    @Getter
    List<Long> emptyAddresses;

    public InspectAddressesResponse(ByteBuf buf) {
        emptyAddresses = ICorfuPayload.listFromBuffer(buf, Long.class);
    }

    public InspectAddressesResponse() {
        emptyAddresses = new ArrayList<>();
    }

    public void add(long address) {
        emptyAddresses.add(address);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, emptyAddresses);
    }
}
