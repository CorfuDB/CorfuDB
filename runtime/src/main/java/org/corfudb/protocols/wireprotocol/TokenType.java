package org.corfudb.protocols.wireprotocol;

import lombok.RequiredArgsConstructor;

/**
 * An enum for distinguishing different response from the sequencer.
 * Created by dalia on 4/8/17.
 */
@RequiredArgsConstructor
public enum TokenType {

    // Standard token issue by sequencer or a tail-query response
    NORMAL((byte) 0),

    // Token request for optimistic TX-commit rejected due to conflict
    TX_ABORT_CONFLICT((byte) 1),

    // Token request for optimistic TX-commit rejected due to a
    // failover-sequencer lacking conflict-resolution info
    TX_ABORT_NEWSEQ((byte) 2),

    // Sent when a transaction aborts a transaction due to missing information
    // (required data evicted from cache)
    TX_ABORT_SEQ_OVERFLOW((byte) 3),

    // Sent when a transaction aborts because it has an old version (i.e. older than
    // the trim mark). This is to detect slow transactions
    TX_ABORT_SEQ_TRIM((byte) 4),

    // Sent when a transaction aborts because it attempts to create a stream that
    // already exists in sequencer. This is to detect when a client tries to reopen
    // an already trimmed stream.
    TX_ABORT_INVALID_STREAM((byte) 5);

    final int val;
}
