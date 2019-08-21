package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

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
    Map<UUID, Long> compactionMarks;

    public ReadResponse(ByteBuf buf) {
        addresses = ICorfuPayload.mapFromBuffer(buf, Long.class, LogData.class);
        compactionMarks = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
    }

    public ReadResponse() {
        addresses = new HashMap<>();
        compactionMarks = new HashMap<>();
    }

    public void put(Long address, LogData data) {
        addresses.put(address, data);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
        ICorfuPayload.serialize(buf, compactionMarks);
    }
}
