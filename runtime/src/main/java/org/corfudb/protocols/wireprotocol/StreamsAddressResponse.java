package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Data;

import java.util.Map;
import java.util.UUID;

/**
 * Represents the response sent by the sequencer when streams address maps are requested
 * @see org.corfudb.protocols.wireprotocol.StreamsAddressRequest
 *
 * It contains a per stream map with its corresponding address space
 * (composed of the addresses of this stream and trim mark)
 */
@Data
public class StreamsAddressResponse implements ICorfuPayload<StreamsAddressResponse>{

    final Map<UUID, StreamAddressSpace> streamsAddressSpaceMap;

    public StreamsAddressResponse(Map<UUID, StreamAddressSpace> streamsAddressesMap) {
        this.streamsAddressSpaceMap = streamsAddressesMap;
    }

    /**
     * Deserialization Constructor from Bytebuf to StreamsAddressResponse.
     *
     * @param buf The buffer to deserialize
     */
    public StreamsAddressResponse(ByteBuf buf) {
        this.streamsAddressSpaceMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, StreamAddressSpace.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, this.streamsAddressSpaceMap);
    }
}
