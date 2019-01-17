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
public class TokenResponse implements ICorfuPayload<TokenResponse>, IToken {

    public static byte[] NO_CONFLICT_KEY = new byte[]{};
    public static UUID NO_CONFLICT_STREAM = new UUID(0L, 0L);

    /**
     * Constructor for TokenResponse.
     *
     * @param token token value
     * @param backpointerMap  map of backpointers for all requested streams
     */
    public TokenResponse(Token token, Map<UUID, Long> backpointerMap) {
        respType = TokenType.NORMAL;
        conflictKey = NO_CONFLICT_KEY;
        conflictStreamID = NO_CONFLICT_STREAM;
        this.token = token;
        this.backpointerMap = backpointerMap;
        this.streamTails = Collections.emptyList();
    }

    /** the cause/type of response. */
    final TokenType respType;

    /**
     * In case there is a conflict, signal to the client which key was responsible for the conflict.
     */
    final byte[] conflictKey;

    /**
     * In case there is a conflict, signal to the client which stream was responsible for the conflict.
     */
    final UUID conflictStreamID;

    /** The current token,
     * or overload with "cause address" in case token request is denied. */
    final Token token;

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
        conflictStreamID = ICorfuPayload.fromBuffer(buf, UUID.class);
        Long epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        Long sequence = ICorfuPayload.fromBuffer(buf, Long.class);
        token = new Token(epoch, sequence);
        backpointerMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
        streamTails = ICorfuPayload.listFromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, respType);
        ICorfuPayload.serialize(buf, conflictKey);
        ICorfuPayload.serialize(buf, conflictStreamID);
        ICorfuPayload.serialize(buf, token.getEpoch());
        ICorfuPayload.serialize(buf, this.getSequence());
        ICorfuPayload.serialize(buf, backpointerMap);
        ICorfuPayload.serialize(buf, streamTails);
    }

    @Override
    public long getSequence() {
        return token.getSequence();
    }

    @Override
    public long getEpoch() {
        return token.getEpoch();
    }

}
