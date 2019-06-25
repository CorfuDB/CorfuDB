package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
    Map<Long, LogData> addresses;

    @Getter
    Map<UUID, Long> streamIdToCompactionMark;

    public ReadResponse(ByteBuf buf) {
        addresses = ICorfuPayload.mapFromBuffer(buf, Long.class, LogData.class);
        streamIdToCompactionMark = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
    }

    public ReadResponse() {
        addresses = new HashMap<Long, LogData>();
        streamIdToCompactionMark = new HashMap<UUID, Long>();
    }

    public void put(Long address, LogData data) {
        addresses.put(address, data);
    }

    public void putCompactionMark(UUID streamId, Long compactionMark) {
        streamIdToCompactionMark.put(streamId, compactionMark);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
        ICorfuPayload.serialize(buf, streamIdToCompactionMark);
    }
}
