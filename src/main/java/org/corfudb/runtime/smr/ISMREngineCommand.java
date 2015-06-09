package org.corfudb.runtime.smr;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * Created by mwei on 5/1/15.
 */
@FunctionalInterface
public interface ISMREngineCommand<T> extends Serializable, BiConsumer<T, ISMREngine.ISMREngineOptions<T>>
{

}