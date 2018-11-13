package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/**
 * Created by annym on 11/8/2018.
 */
@Data
@AllArgsConstructor
public class TouchResponse implements ICorfuPayload<TouchResponse> {

    @Getter
    Map<LogicalSequenceNumber, Boolean> addressesTouchMap;

    public TouchResponse(ByteBuf buf) {
        addressesTouchMap = ICorfuPayload.mapFromBuffer(buf, LogicalSequenceNumber.class, Boolean.class);
    }

    public TouchResponse() {
        addressesTouchMap = new HashMap<>();
    }

    public void put(LogicalSequenceNumber address, Boolean exists) {
        addressesTouchMap.put(address, exists);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addressesTouchMap);
    }
}