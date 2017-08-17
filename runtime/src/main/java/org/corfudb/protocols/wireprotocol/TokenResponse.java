package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
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
    public static UUID EMPTY_UUID = new UUID(0L, 0L);

    /**
     * Constructor for TokenResponse.
     *
     * @param tokenValue token value
     * @param epoch current epoch
     * @param backpointerMap  map of backpointers for all requested streams
     */
    public TokenResponse(long tokenValue, long epoch, Map<UUID, Long> backpointerMap) {
        respType = TokenType.NORMAL;
        conflictKey = NO_CONFLICT_KEY;
        token = new Token(tokenValue, epoch);
        this.backpointerMap = backpointerMap;
        this.conflictStream = EMPTY_UUID;
    }

    public TokenResponse(TokenType type, long address, long epoch) {
        respType = type;
        conflictKey = NO_CONFLICT_KEY;
        token = new Token(address, epoch);
        this.backpointerMap = Collections.emptyMap();
        this.conflictStream = EMPTY_UUID;
    }

    public TokenResponse(TokenType type, byte[] conflictKey, UUID conflictStream,
                         long address, long epoch) {
        respType = type;
        this.conflictKey = conflictKey;
        this.conflictStream = conflictStream;
        token = new Token(address, epoch);
        this.backpointerMap = Collections.emptyMap();
    }


    /** the cause/type of response. */
    final TokenType respType;

    /**
     * In case there is a conflict, signal to the client which key was responsible for the conflict.
     */
    final byte[] conflictKey;

    /** The stream that caused the conflict if type is abort. */
    final UUID conflictStream;

    /** The current token,
     * or overload with "cause address" in case token request is denied. */
    final Token token;

    /** The backpointer map, if available. */
    final Map<UUID, Long> backpointerMap;

    /**
     * Deserialization Constructor from a Bytebuf to TokenResponse.
     *
     * @param buf The buffer to deserialize
     */
    public TokenResponse(ByteBuf buf) {
        respType = TokenType.values()[ICorfuPayload.fromBuffer(buf, Byte.class)];
        Long tokenValue = ICorfuPayload.fromBuffer(buf, Long.class);
        Long epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        token = new Token(tokenValue, epoch);

        if (respType.isAborted()) {
            conflictKey = ICorfuPayload.fromBuffer(buf, byte[].class);
            conflictStream = ICorfuPayload.fromBuffer(buf, UUID.class);
            backpointerMap = Collections.emptyMap();
        } else {
            conflictKey = NO_CONFLICT_KEY;
            conflictStream = EMPTY_UUID;
            backpointerMap = ICorfuPayload.mapFromBuffer(buf, UUID.class, Long.class);
        }
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, respType);

        ICorfuPayload.serialize(buf, token.getTokenValue());
        ICorfuPayload.serialize(buf, token.getEpoch());

        if (respType.isAborted()) {
            ICorfuPayload.serialize(buf, conflictKey);
            ICorfuPayload.serialize(buf, conflictStream);
        } else {
            ICorfuPayload.serialize(buf, backpointerMap);
        }
    }

    @Override
    public long getTokenValue() {
        return token.getTokenValue();
    }

    @Override
    public long getEpoch() {
        return token.getEpoch();
    }

}
