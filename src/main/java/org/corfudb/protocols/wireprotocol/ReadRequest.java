package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
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

    final RangeSet<Long> addresses;
    final UUID streamID;

    public ReadRequest(ByteBuf buf) {
        addresses = ICorfuPayload.rangeSetFromBuffer(buf, Long.class);
        if (ICorfuPayload.fromBuffer(buf, Boolean.class)) {
            streamID = ICorfuPayload.fromBuffer(buf, UUID.class);
        }
        else {
            streamID = null;
        }
    }

    public ReadRequest(Long address) {
        addresses = ImmutableRangeSet.of(Range.singleton(address));
        streamID = null;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, addresses);
        ICorfuPayload.serialize(buf, streamID != null);
        if (streamID != null) {
            ICorfuPayload.serialize(buf, streamID);
        }
    }

}
