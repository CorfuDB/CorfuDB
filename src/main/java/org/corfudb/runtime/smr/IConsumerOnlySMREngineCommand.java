package org.corfudb.runtime.smr;

import java.io.Serializable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Created by mwei on 6/11/15.
 */
@FunctionalInterface
public interface IConsumerOnlySMREngineCommand<T> extends Serializable, BiConsumer<T, ISMREngine.ISMREngineOptions<T>>,
        ISMREngineCommand<T, Void> {

    /**
     * Applies this function to the given arguments.
     *
     * @param t                  the first function argument
     * @param tismrEngineOptions the second function argument
     * @return the function result
     */
    @Override
    default Void apply(T t, ISMREngine.ISMREngineOptions<T> tismrEngineOptions) {
        this.accept(t, tismrEngineOptions);
        return null;
    }
}
