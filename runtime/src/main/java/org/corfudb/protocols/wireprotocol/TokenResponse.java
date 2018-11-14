package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by mwei on 8/8/16.
 */
@Data
@AllArgsConstructor
public class TokenResponse implements ICorfuPayload<TokenResponse>, Token {

    public static byte[] NO_CONFLICT_KEY = new byte[]{};

    /**
     * Constructor for TokenResponse.
     *
     * @param LSN LSN value
     * @param backpointerMap  map of backpointers for all requested streams
     */
    public TokenResponse(LSN LSN, Map<UUID, Long> backpointerMap) {
        respType = TokenType.NORMAL;
        conflictKey = NO_CONFLICT_KEY;
        this.lsn = LSN;
        this.backpointerMap = backpointerMap;
        this.streamTails = Collections.emptyList();
    }

    /** the cause/type of response. */
    final TokenType respType;

    /**
     * In case there is a conflict, signal to the client which key was responsible for the conflict.
     */
    final byte[] conflictKey;

    /** The current LSN,
     * or overload with "cause address" in case LSN request is denied. */
    final LSN lsn;

    /** The backpointer map, if available. */
    final Map<UUID, Long> backpointerMap;

    final List<Long> streamTails;

    /**
     * Deserialization Constructor from a Bytebuf to TokenResponse.
     *
     * @param buf The buffer to deserialize
     */
    public TokenResponse(ByteBuf buf) {
        respType = TokenType.values()[ICorfuPayload.fromBuffer(buf, Byte.class)];
        conflictKey = ICorfuPayload.fromBuffer(buf, byte[].class);
        Long epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        Long sequence = ICorfuPayload.fromBuffer(buf, Long.class);
        lsn = new LSN(epoch, sequence);
        backpointerMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
        streamTails = ICorfuPayload.listFromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, respType);
        ICorfuPayload.serialize(buf, conflictKey);
        ICorfuPayload.serialize(buf, lsn.getEpoch());
        ICorfuPayload.serialize(buf, lsn.getSequence());
        ICorfuPayload.serialize(buf, backpointerMap);
        ICorfuPayload.serialize(buf, streamTails);
    }

    @Override
    public LSN getLSN() {
        return lsn;
    }
}
