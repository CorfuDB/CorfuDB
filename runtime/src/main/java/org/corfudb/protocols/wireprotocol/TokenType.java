package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** An enum for distinguishing different response from the sequencer.
 * Created by dalia on 4/8/17.
 */
@RequiredArgsConstructor
public enum  TokenType implements ICorfuPayload<TokenType> {

    // standard token issue by sequencer
    NORMAL((byte) 0, false),

    // response to tail-query (no allocation)
    QUERY((byte)1, false),

    // token request for optimistic TX-commit rejected due to conflict
    TX_ABORT_CONFLICT_KEY((byte)2, true),

    // token request for optimistic TX-commit rejected due to a
    // failover-sequencer lacking conflict-resolution info
    TX_ABORT_NEWSEQ((byte) 3, false),

    // Sent when a transaction aborts a transaction due to missing information
    // (required data evicted from cache)
    TX_ABORT_SEQ_OVERFLOW((byte) 4, false),

    // Sent when a transaction aborts because it has an old version (i.e. older than
    // the trim mark). This is to detect slow transactions
    TX_ABORT_SEQ_TRIM((byte) 5, false),

    /** Token request for optimistic transaction commit rejected due to
     *  stream address conflict (no keys available).
     */
    TX_ABORT_CONFLICT_STREAM((byte) 6, true);

    final int val;

    @Getter
    final boolean aborted;

    byte asByte() {
        return (byte) val;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeByte(asByte());
    }

    static Map<Byte, TokenType> typeMap =
            Arrays.stream(TokenType.values())
                    .collect(Collectors.toMap(TokenType::asByte, Function.identity()));

}