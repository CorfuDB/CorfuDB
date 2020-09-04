package org.corfudb.protocols.wireprotocol;


import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collections;
import java.util.List;

/**
 * Created by mwei on 8/11/16.
 */
@Getter
@AllArgsConstructor
public class ReadRequest implements ICorfuPayload<ReadRequest> {

    // List of requested addresses to read.
    private final List<Long> addresses;

    // Whether the read results should be cached on server.
    private final boolean cacheReadResult;

    public ReadRequest(long address, boolean cacheReadResult) {
        this.addresses = Collections.singletonList(address);
        this.cacheReadResult = cacheReadResult;
    }


    /**
     * Deserialization Constructor from ByteBuf to ReadRequest.
     *
     * @param buf The buffer to deserialize
     */
    public ReadRequest(ByteBuf buf) {
        addresses = ICorfuPayload.listFromBuffer(buf, Long.class);
        cacheReadResult = buf.readBoolean();
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
        buf.writeBoolean(cacheReadResult);
    }

}
