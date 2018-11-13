package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.Range;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by mwei on 8/11/16.
 */
@Data
@AllArgsConstructor
public class ReadRequest implements ICorfuPayload<ReadRequest> {

    final Range<LogicalSequenceNumber> range;

    /**
     * Deserialization Constructor from ByteBuf to ReadRequest.
     *
     * @param buf The buffer to deserialize
     */
    public ReadRequest(ByteBuf buf) {
        range = ICorfuPayload.rangeFromBuffer(buf, LogicalSequenceNumber.class);
    }

    public ReadRequest(LogicalSequenceNumber address) {
        range = Range.singleton(address);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, range);
    }

}
