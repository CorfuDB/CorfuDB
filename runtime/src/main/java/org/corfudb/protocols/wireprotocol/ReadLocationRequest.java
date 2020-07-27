package org.corfudb.protocols.wireprotocol;


import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.runtime.collections.LocationBucket;
import org.corfudb.runtime.collections.LocationBucket.LocationImpl;

import java.util.Collections;
import java.util.List;

/**
 * Created by mwei on 8/11/16.
 */
@Getter
@AllArgsConstructor
public class ReadLocationRequest implements ICorfuPayload<ReadRequest> {

    // List of requested addresses to read.
    private final List<LocationImpl> locations;

    // Whether the read results should be cached on server.
    private final boolean cacheReadResult;

    public ReadLocationRequest(LocationImpl location, boolean cacheReadResult) {
        this.locations = Collections.singletonList(location);
        this.cacheReadResult = cacheReadResult;
    }


    /**
     * Deserialization Constructor from ByteBuf to ReadRequest.
     *
     * @param buf The buffer to deserialize
     */
    public ReadLocationRequest(ByteBuf buf) {
        locations = ICorfuPayload.listFromBuffer(buf, LocationImpl.class);
        cacheReadResult = buf.readBoolean();
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, locations);
        buf.writeBoolean(cacheReadResult);
    }

}
