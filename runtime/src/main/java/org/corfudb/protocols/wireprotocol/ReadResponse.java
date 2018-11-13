package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

/**
 * Created by mwei on 8/15/16.
 */
@Data
@AllArgsConstructor
public class ReadResponse implements ICorfuPayload<ReadResponse> {

    @Getter
    Map<LogicalSequenceNumber, LogData> addresses;

    public ReadResponse(ByteBuf buf) {
        addresses = ICorfuPayload.mapFromBuffer(buf, LogicalSequenceNumber.class, LogData.class);
    }

    public ReadResponse() {
        addresses = new HashMap<LogicalSequenceNumber, LogData>();
    }

    public void put(LogicalSequenceNumber address, LogData data) {
        addresses.put(address, data);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
    }
}
