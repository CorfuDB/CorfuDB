package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.List;

/**
 * A request to read multiple addresses.
 *
 * Created by maithem on 7/28/17.
 */
@Getter
@AllArgsConstructor
public class MultipleReadRequest implements ICorfuPayload<MultipleReadRequest> {

    // List of requested addresses to read.
    private final List<Long> addresses;

    // Whether the read results should be cached on server.
    private final boolean cacheReadResult;

    /**
     * Deserialization Constructor from ByteBuf to ReadRequest.
     *
     * @param buf The buffer to deserialize
     */
    public MultipleReadRequest(ByteBuf buf) {
        addresses = ICorfuPayload.listFromBuffer(buf, Long.class);
        cacheReadResult = buf.readBoolean();
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
        buf.writeBoolean(cacheReadResult);
    }
}
