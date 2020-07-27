package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.corfudb.runtime.collections.LocationBucket;
import org.corfudb.runtime.collections.LocationBucket.LocationImpl;

/**
 * Created by mwei on 8/15/16.
 */
@Data
@AllArgsConstructor
public class ReadLocationResponse implements ICorfuPayload<ReadResponse> {

    @Getter
    Map<LocationImpl, LogData> locations;

    public ReadLocationResponse(ByteBuf buf) {
        locations = ICorfuPayload.mapFromBuffer(buf, LocationImpl.class, LogData.class);
    }

    public ReadLocationResponse() {
        locations = new HashMap<>();
    }

    public void put(LocationImpl location, LogData data) {
        locations.put(location, data);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, locations);
    }
}
