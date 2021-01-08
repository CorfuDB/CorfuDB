package org.corfudb.protocols.wireprotocol;

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * Created by mwei on 8/8/16.
 */
@Data
@AllArgsConstructor
public class TokenResponse implements IToken {

    public static byte[] NO_CONFLICT_KEY = new byte[]{0};
    public static UUID NO_CONFLICT_STREAM = new UUID(0, 0);

    /**
     * Constructor for TokenResponse.
     *
     * @param token          token value
     * @param backpointerMap map of backpointers for all requested streams
     */
    public TokenResponse(Token token, Map<UUID, Long> backpointerMap) {
        this.respType = TokenType.NORMAL;
        this.conflictKey = NO_CONFLICT_KEY;
        this.conflictStream = NO_CONFLICT_STREAM;
        this.token = token;
        this.backpointerMap = backpointerMap;
        this.streamTails = Collections.emptyMap();
    }

    /** the cause/type of response. */
    final TokenType respType;

    // In case of a conflict, signal to the client which key was responsible for the conflict.
    final byte[] conflictKey;

    // In case of a conflict, signal to the client which stream was responsible for the conflict.
    final UUID conflictStream;

    /** The current token or global log tail in the case of stream tails query */
    final Token token;

    /** The backpointer map, if available. */
    final Map<UUID, Long> backpointerMap;

    @Getter(AccessLevel.NONE)
    final Map<UUID, Long> streamTails;

    @Override
    public long getSequence() {
        return token.getSequence();
    }

    @Override
    public long getEpoch() {
        return token.getEpoch();
    }

    public Long getStreamTail(UUID streamId) {
        return streamTails.get(streamId);
    }

    @VisibleForTesting
    public int getStreamTailsCount() {
        return streamTails.size();
    }
}
