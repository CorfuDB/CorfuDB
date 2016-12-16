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

    /** The streams which are written to by this token request. */
    final Set<UUID> streams;

    /* True if the Replex protocol encountered an overwrite at the global log layer. */
    final Boolean overwrite;

    /* True if the Replex protocol encountered an overwrite at the local stream layer. */
    final Boolean replexOverwrite;

    /* The following 3 variables are used for transaction resolution. */
    final Boolean txnResolution;

    /* Latest readstamp of the txn. */
    final Long readTimestamp;

    /*
     * Streams that are in the read set of the txn. The txn can only commit if none of the current offsets in each of
     * streams is greater than readTimestamp.
     */
    final Set<UUID> readSet;

    public TokenRequest(Long numTokens, Set<UUID> streams, Boolean overwrite, Boolean replexOverwrite) {
        this.numTokens = numTokens;
        this.streams = streams;
        this.overwrite = overwrite;
        this.replexOverwrite = replexOverwrite;
        txnResolution = false;
        readTimestamp = -1L;
        readSet = null;
    }

    public TokenRequest(ByteBuf buf) {
        numTokens = ICorfuPayload.fromBuffer(buf, Long.class);
        if (ICorfuPayload.fromBuffer(buf, Boolean.class))
            streams = ICorfuPayload.setFromBuffer(buf, UUID.class);
        else streams = null;
        overwrite = ICorfuPayload.fromBuffer(buf, Boolean.class);
        replexOverwrite = ICorfuPayload.fromBuffer(buf, Boolean.class);
        txnResolution = ICorfuPayload.fromBuffer(buf, Boolean.class);
        if (txnResolution) {
            readTimestamp = ICorfuPayload.fromBuffer(buf, Long.class);
            readSet = ICorfuPayload.setFromBuffer(buf, UUID.class);
        }
        else {
            readTimestamp = -1L;
            readSet = null;
        }
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
            ICorfuPayload.serialize(buf, readSet);
        }
    }
}