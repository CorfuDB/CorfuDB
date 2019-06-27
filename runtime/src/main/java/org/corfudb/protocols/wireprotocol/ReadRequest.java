package org.corfudb.protocols.wireprotocol;


import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by mwei on 8/11/16.
 */
@Getter
@AllArgsConstructor
public class ReadRequest implements ICorfuPayload<ReadRequest> {

    // Requested address to read.
    private final long address;

    // Whether the read result should be cached on server.
    private final boolean cacheReadResult;

    /**
     * Deserialization Constructor from ByteBuf to ReadRequest.
     *
     * @param buf The buffer to deserialize
     */
    public ReadRequest(ByteBuf buf) {
        address = buf.readLong();
        cacheReadResult = buf.readBoolean();
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeLong(address);
        buf.writeBoolean(cacheReadResult);
    }

}
