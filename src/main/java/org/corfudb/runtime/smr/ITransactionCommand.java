package org.corfudb.runtime.smr;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Created by mwei on 5/4/15.
 */
@FunctionalInterface
public interface ITransactionCommand extends Supplier<Boolean>, Serializable
{

}