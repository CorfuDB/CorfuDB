package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.view.Address;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mwei on 8/15/16.
 */
@Data
@AllArgsConstructor
public class ReadResponse implements ICorfuPayload<ReadResponse> {

    @Getter
    Map<Long, LogData> addresses;

    @Setter
    @Getter
    long compactionMark;

    public ReadResponse(ByteBuf buf) {
        addresses = ICorfuPayload.mapFromBuffer(buf, Long.class, LogData.class);
        compactionMark = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    public ReadResponse() {
        addresses = new HashMap<>();
        compactionMark = Address.NON_ADDRESS;
    }

    public void put(Long address, LogData data) {
        addresses.put(address, data);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
        ICorfuPayload.serialize(buf, compactionMark);
    }
}