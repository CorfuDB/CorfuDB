package org.corfudb.runtime.view;

/**
 * Created by mwei on 3/24/16.
 */
public enum TransactionStrategy {
    AUTOMATIC,          // Automatically decide.
    OPTIMISTIC,
    LOCKING,
    SNAPSHOT_REF,       // Snapshot TXn by using a function reference.
    SNAPSHOT_LAMBDA     // Snapshot TXn by using a function lambda.
}
