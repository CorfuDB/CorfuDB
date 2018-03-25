package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BatchTokenResponse implements ICorfuPayload<BatchTokenResponse> {

    @Getter
    final Map<UUID, Long> addressMap;

    @Getter
    final long globalTail;

    @Getter
    final long epoch;

    @RequiredArgsConstructor
    public static class BackpointerToken implements ICorfuPayload<BackpointerToken> {
        @Getter
        public final long token;

        @Getter
        public final Map<UUID, Long> backpointerMap;

        public BackpointerToken (ByteBuf buf) {
            token = ICorfuPayload.fromBuffer(buf, long.class);
            backpointerMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, long.class);
        }

        @Override
        public void doSerialize(ByteBuf buf) {
            ICorfuPayload.serialize(buf, token);
            ICorfuPayload.serialize(buf, backpointerMap);
        }
    }

    @Getter
    final List<BackpointerToken> unconditionalTokens;

    @Getter
    final List<BackpointerToken> conditionalTokens;

    public BatchTokenResponse(ByteBuf buf) {
        globalTail = ICorfuPayload.fromBuffer(buf, long.class);
        epoch = ICorfuPayload.fromBuffer(buf, long.class);
        addressMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
        unconditionalTokens = ICorfuPayload.listFromBuffer(buf, BackpointerToken.class);
        conditionalTokens = ICorfuPayload.listFromBuffer(buf, BackpointerToken.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, globalTail);
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, addressMap);
        ICorfuPayload.serialize(buf, unconditionalTokens);
        ICorfuPayload.serialize(buf, conditionalTokens);
    }
}
