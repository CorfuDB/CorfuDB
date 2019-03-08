package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Data;

import java.util.List;

/**
 * Represents the request sent to the sequencer to retrieve one or several streams address map.
 *
 * Created by annym on 02/06/2019
 */
@CorfuPayload
@Data
public class StreamsAddressRequest implements ICorfuPayload<StreamsAddressRequest>{

    final List<StreamAddressRange> streamsRanges;

    public StreamsAddressRequest(List<StreamAddressRange> streamsRanges) {
        this.streamsRanges = streamsRanges;
    }

    /**
     * Deserialization Constructor from Bytebuf to StreamsAddressRequest.
     *
     * @param buf The buffer to deserialize
     */
    public StreamsAddressRequest(ByteBuf buf) {
        this.streamsRanges = ICorfuPayload.listFromBuffer(buf, StreamAddressRange.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, this.streamsRanges);
    }
}
