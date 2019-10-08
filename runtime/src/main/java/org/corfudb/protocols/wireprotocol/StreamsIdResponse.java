package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

import java.util.List;
import java.util.UUID;

/**
 * Represents the response to sent by the sequencer when IDs of streams are requested.
 */
public class StreamsIdResponse implements ICorfuPayload<StreamsIdResponse> {

    @Getter
    private final List<UUID> streamIds;

    public StreamsIdResponse(List<UUID> streamIds) {
        this.streamIds = streamIds;
    }

    /**
     * Deserialization Constructor from Bytebuf to StreamsIdResponse.
     *
     * @param buf The buffer to deserialize
     */
    public StreamsIdResponse(ByteBuf buf) {
        this.streamIds = ICorfuPayload.listFromBuffer(buf, UUID.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, this.streamIds);
    }
}
