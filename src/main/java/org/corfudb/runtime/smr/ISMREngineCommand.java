package org.corfudb.runtime.smr;

import java.io.Serializable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Created by mwei on 5/1/15.
 */
@FunctionalInterface
public interface ISMREngineCommand<T, R> extends Serializable, BiFunction<T, ISMREngine.ISMREngineOptions<T>, R>
{

}