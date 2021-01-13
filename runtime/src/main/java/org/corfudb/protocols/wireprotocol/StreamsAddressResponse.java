package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.Map;
import java.util.UUID;

/**
 * Represents the response sent by the sequencer when streams address maps are requested
 *
 * @see org.corfudb.runtime.proto.service.Sequencer.StreamsAddressRequestMsg
 * <p>
 * It contains a per stream map with its corresponding address space
 * (composed of the addresses of this stream and trim mark)
 */
@Data
public class StreamsAddressResponse implements ICorfuPayload<StreamsAddressResponse> {

    private long logTail;

    private long epoch = Layout.INVALID_EPOCH;

    private final Map<UUID, StreamAddressSpace> addressMap;

    public StreamsAddressResponse(long logTail, Map<UUID, StreamAddressSpace> streamsAddressesMap) {
        this.logTail = logTail;
        this.addressMap = streamsAddressesMap;
    }

    /**
     * Deserialization Constructor from Bytebuf to StreamsAddressResponse.
     *
     * Note: LogUnitServer also needs to serialize a StreamsAddressResponse, so
     * this methods can be removed once both of the servers are using Protobuf.
     *
     * @param buf The buffer to deserialize
     */
    public StreamsAddressResponse(ByteBuf buf) {
        this.logTail = ICorfuPayload.fromBuffer(buf, Long.class);
        this.epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        this.addressMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, StreamAddressSpace.class);
    }

    /**
     * Serialize a the object and append to the {@link ByteBuf}  parameter passed.
     *
     * Note: LogUnitServer also needs to serialize a StreamsAddressResponse, so
     * this methods can be removed once both of the servers are using Protobuf.
     *
     * @param buf the {@link ByteBuf} to which the serialized bytes are appended
     */
    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, this.logTail);
        ICorfuPayload.serialize(buf, this.epoch);
        ICorfuPayload.serialize(buf, this.addressMap);
    }
}
