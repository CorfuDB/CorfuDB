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
 * Created by WenbinZhu on 9/25/19.
 */
@Data
@AllArgsConstructor
public class InspectAddressesResponse implements ICorfuPayload<InspectAddressesResponse> {

    @Getter
    List<Long> holeAddresses;

    public InspectAddressesResponse(ByteBuf buf) {
        holeAddresses = ICorfuPayload.listFromBuffer(buf, Long.class);
    }

    public InspectAddressesResponse() {
        holeAddresses = new ArrayList<>();
    }

    public void add(long address) {
        holeAddresses.add(address);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, holeAddresses);
    }
}
