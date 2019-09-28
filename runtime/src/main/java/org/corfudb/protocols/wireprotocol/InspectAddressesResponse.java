package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by WenbinZhu on 9/25/19.
 */
@Data
@AllArgsConstructor
public class InspectAddressesResponse implements ICorfuPayload<InspectAddressesResponse> {

    @Getter
    Map<Long, Boolean> addresses;

    public InspectAddressesResponse(ByteBuf buf) {
        addresses = ICorfuPayload.mapFromBuffer(buf, Long.class, Boolean.class);
    }

    public InspectAddressesResponse() {
        addresses = new HashMap<>();
    }

    public void put(long address, boolean exists) {
        addresses.put(address, exists);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
    }
}
