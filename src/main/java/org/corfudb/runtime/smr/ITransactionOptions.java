package org.corfudb.runtime.smr;

import org.corfudb.runtime.entries.CorfuDBEntry;

import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 5/4/15.
 */
public interface ITransactionOptions {
    default void abort()
    {
        throw new TransactionAbortedException();
    }
}
