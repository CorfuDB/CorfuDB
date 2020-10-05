package org.corfudb.runtime.collections;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuStoreMetadata;

/**
 * Used to specify which transaction isolation level will be used for the TxnContext transactions.
 * Following isolation levels are supported:
 *  1. SNAPSHOT() - all writes and reads will happen at a specific timestamp
 *      1.a - none (DEFAULT): Latest timestamp is taken on first read operation
 *            or on commit in the absence of reads.
 *      1.b - timestamp: on which the transaction needs to be validated can also be explicitly provided
 *            for SNAPSHOT isolation.
 *
 *  created by hisundar on 2020-09-09
 */
public class IsolationLevel {
    @Getter
    private Token timestamp;
    // Initialize this class using one of the following Isolation types
    private IsolationLevel(Token timestamp) {
        this.timestamp = timestamp;
    }

    public static IsolationLevel snapshot() {
        return new IsolationLevel(Token.UNINITIALIZED);
    }

    public static IsolationLevel snapshot(CorfuStoreMetadata.Timestamp timestamp) {
        return new IsolationLevel(new Token(timestamp.getEpoch(), timestamp.getSequence()));
    }
}
