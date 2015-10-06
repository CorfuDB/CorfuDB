package org.corfudb.runtime.smr;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by mwei on 5/4/15.
 */
@FunctionalInterface
public interface ITransactionCommand<R> extends Function<Object, R>, Serializable
{

}