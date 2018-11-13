package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by annym on 11/8/2018.
 */
@Data
@AllArgsConstructor
public class TouchRequest implements ICorfuPayload<TouchRequest> {

    final List<LogicalSequenceNumber> addresses;

    /**
     * Deserialization Constructor from ByteBuf to ReadRequest.
     *
     * @param buf The buffer to deserialize
     */
    public TouchRequest(ByteBuf buf) {
        addresses = ICorfuPayload.listFromBuffer(buf, LogicalSequenceNumber.class);
    }

    public TouchRequest(LogicalSequenceNumber address) { addresses = Arrays.asList(address); }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
    }

}
