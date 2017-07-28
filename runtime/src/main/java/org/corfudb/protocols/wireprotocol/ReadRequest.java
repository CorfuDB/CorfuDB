package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by mwei on 8/11/16.
 */
@Data
@AllArgsConstructor
public class ReadRequest implements ICorfuPayload<ReadRequest> {

    final List<Long> list;

    /**
     * Deserialization Constructor from ByteBuf to ReadRequest.
     *
     * @param buf The buffer to deserialize
     */
    public ReadRequest(ByteBuf buf) {
        list = ICorfuPayload.listFromBuffer(buf, Long.class);
    }

    public ReadRequest(Long address) {
        list = Arrays.asList(address);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, list);
    }

}
