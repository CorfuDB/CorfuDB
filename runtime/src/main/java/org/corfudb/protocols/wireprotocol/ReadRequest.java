package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.UUID;

/**
 * Created by mwei on 8/11/16.
 */
@Data
@AllArgsConstructor
public class ReadRequest implements ICorfuPayload<ReadRequest> {

    final Range<Long> range;
    final UUID streamID;
    final Long forwardPointer;

    public ReadRequest(ByteBuf buf) {
        range = ICorfuPayload.rangeFromBuffer(buf, Long.class);
        if (ICorfuPayload.fromBuffer(buf, Boolean.class)) {
            streamID = ICorfuPayload.fromBuffer(buf, UUID.class);
        }
        else {
            streamID = null;
        }
        if (ICorfuPayload.fromBuffer(buf, Boolean.class)) {
            forwardPointer = ICorfuPayload.fromBuffer(buf, Long.class);
        } else {
            forwardPointer = new Long(0);
        }
    }

    public ReadRequest(Long address) {
        this(address, new Long(0));
    }

    public ReadRequest(Long address, Long next_address) {
        range = Range.singleton(address);
        streamID = null;
        forwardPointer = next_address;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, range);
        ICorfuPayload.serialize(buf, streamID != null);
        if (streamID != null) {
            ICorfuPayload.serialize(buf, streamID);
        }
        ICorfuPayload.serialize(buf, forwardPointer > 0);
        if (forwardPointer > 0) {
            ICorfuPayload.serialize(buf, forwardPointer);
        }
    }

}
