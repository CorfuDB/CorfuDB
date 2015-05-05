package org.corfudb.runtime.smr;

import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 5/4/15.
 */
public interface ITransactionOptions {
    CompletableFuture<Object> getReturnResult();
}
