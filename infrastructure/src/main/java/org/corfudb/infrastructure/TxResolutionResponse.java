package org.corfudb.infrastructure;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
import org.corfudb.runtime.view.Address;

import java.util.UUID;

/**
 * A simple data-class that is used by {@link SequencerServer#txnCanCommit} to inform the caller
 * about the result of transaction conflict resolution.
 */
@Data
@ToString
@AllArgsConstructor
class TxResolutionResponse {

    /**
     * A convenience constructor that has all optional paramaters set to their default values.
     *
     * @param tokenType response type from the sequencer
     */
    public TxResolutionResponse(TokenType tokenType) {
        this.tokenType = tokenType;
        this.address = Address.NON_ADDRESS;
        this.conflictingKey = TokenResponse.NO_CONFLICT_KEY;
        this.conflictingStream = TokenResponse.NO_CONFLICT_STREAM;
    }

    /**
     * Response type from the sequencer.
     */
    TokenType tokenType;

    /**
     * Optional. In case of a conflict, what was the address at which that conflict occurred.
     */
    Long address;

    /**
     * Optional. In case of a conflict, what was the key that triggered a negative response.
     */
    byte[] conflictingKey;

    /**
     * Optional. In case of a conflict, what was the stream ID that triggered a negative response.
     */
    UUID conflictingStream;
}
