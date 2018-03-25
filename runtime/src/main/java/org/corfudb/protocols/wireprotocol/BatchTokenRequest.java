package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BatchTokenRequest implements ICorfuPayload<BatchTokenRequest> {

    @Getter
    final UUID[] readStreams;

    @Getter
    final List<UUID[]> unconditionalTokenRequests;

    @Getter
    final List<TxResolutionInfo> conditionalTokenRequests;

    public BatchTokenRequest(ByteBuf buf) {
        readStreams = ICorfuPayload.arrayFromBuffer(buf, UUID.class);
        unconditionalTokenRequests = ICorfuPayload.listFromBuffer(buf, UUID[].class);
        conditionalTokenRequests = ICorfuPayload.listFromBuffer(buf, TxResolutionInfo.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, readStreams);
        ICorfuPayload.serialize(buf, unconditionalTokenRequests);
        ICorfuPayload.serialize(buf, conditionalTokenRequests);
    }
}
