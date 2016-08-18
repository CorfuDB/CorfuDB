package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;
import java.util.UUID;

/**
 * Created by mwei on 8/8/16.
 */
@Data
@AllArgsConstructor
public class TokenResponse implements ICorfuPayload<TokenResponse> {

    /** The current token. */
    final Long token;

    /** The backpointer map, if available. */
    final Map<UUID, Long> backpointerMap;

    public TokenResponse(ByteBuf buf) {
        token = ICorfuPayload.fromBuffer(buf, Long.class);
        backpointerMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, token);
        ICorfuPayload.serialize(buf, backpointerMap);
    }
}
