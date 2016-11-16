package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Created by mwei on 8/9/16.
 */
@Data
@AllArgsConstructor
public class TokenRequest implements ICorfuPayload<TokenRequest> {

    /** The number of tokens to request. 0 means to request the current token. */
    final Long numTokens;

    /** The streams which are affected by this token request. */
    final Set<UUID> streams;

    /* True if the Replex protocol encountered an overwrite at the global log layer. */
    final Boolean overwrite;

    /* True if the Replex protocol encountered an overwrite at the local stream layer. */
    final Boolean replexOverwrite;

    final Boolean txnResolution;

    final Long readTimestamp;

    public TokenRequest(Long numTokens, Set<UUID> streams, Boolean overwrite, Boolean replexOverwrite) {
        this.numTokens = numTokens;
        this.streams = streams;
        this.overwrite = overwrite;
        this.replexOverwrite = replexOverwrite;
        txnResolution = false;
        readTimestamp = -1L;
    }

    public TokenRequest(ByteBuf buf) {
        numTokens = ICorfuPayload.fromBuffer(buf, Long.class);
        if (ICorfuPayload.fromBuffer(buf, Boolean.class))
            streams = ICorfuPayload.setFromBuffer(buf, UUID.class);
        else streams = null;
        overwrite = ICorfuPayload.fromBuffer(buf, Boolean.class);
        replexOverwrite = ICorfuPayload.fromBuffer(buf, Boolean.class);
        txnResolution = ICorfuPayload.fromBuffer(buf, Boolean.class);
        if (txnResolution)
            readTimestamp = ICorfuPayload.fromBuffer(buf, Long.class);
        else readTimestamp = -1L;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, numTokens);
        ICorfuPayload.serialize(buf, streams != null);
        if (streams != null)
            ICorfuPayload.serialize(buf, streams);
        ICorfuPayload.serialize(buf, overwrite);
        ICorfuPayload.serialize(buf, replexOverwrite);
        ICorfuPayload.serialize(buf, txnResolution);
        if (txnResolution) {
            ICorfuPayload.serialize(buf, readTimestamp);
        }
    }
}
