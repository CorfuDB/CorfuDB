package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.*;

/**
 * Created by mwei on 8/15/16.
 */
@Data
@AllArgsConstructor
public class ReadResponse implements ICorfuPayload<ReadResponse> {

    @Getter
    Map<Long, LogData> readSet;

    public ReadResponse(ByteBuf buf) {
        readSet = ICorfuPayload.mapFromBuffer(buf, Long.class, LogData.class);
    }

    public ReadResponse() {
        readSet = new HashMap<Long, LogData>();
    }

    public void put(Long address, LogData data) {
        readSet.put(address, data);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, readSet);
    }
}
