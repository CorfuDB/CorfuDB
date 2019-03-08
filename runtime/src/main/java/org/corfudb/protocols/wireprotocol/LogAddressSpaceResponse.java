package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Data;

import java.util.Map;
import java.util.UUID;

/**
 * Represents the response sent by the Log Unit when the log's address space is requested
 *
 * It contains:
 *     1. A per stream map with each stream's corresponding address space
 *       (composed of the addresses of this stream and trim mark)
 *     2. Global Log Tail.
 */
@Data
public class LogAddressSpaceResponse implements ICorfuPayload<LogAddressSpaceResponse>{

    final long logTail;
    final Map<UUID, StreamAddressSpace> streamsAddressSpace;

    public LogAddressSpaceResponse(long globalTail, Map<UUID, StreamAddressSpace> streamsAddressesMap) {
        this.logTail = globalTail;
        this.streamsAddressSpace = streamsAddressesMap;
    }

    /**
     * Deserialization Constructor from Bytebuf to StreamsAddressResponse.
     *
     * @param buf The buffer to deserialize
     */
    public LogAddressSpaceResponse(ByteBuf buf) {
        this.logTail = ICorfuPayload.fromBuffer(buf, Long.class);
        this.streamsAddressSpace = ICorfuPayload.mapFromBuffer(buf, UUID.class, StreamAddressSpace.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, this.logTail);
        ICorfuPayload.serialize(buf, this.streamsAddressSpace);
    }
}