package org.corfudb.runtime.object;

/** Interface for implementing functions which directly
 *  read.
 *
 *  @param <R> The return type of the function.
 *
 * Created by mwei on 5/19/17.
 */
@FunctionalInterface
public interface IDirectAccessFunction<R> {

    /** Compute the result for a direct access.
     *
     * @param accessArgs    The arguments to the accessor
     * @param mutatorArgs   The arguments from the mutator
     * @return              The return value, as if SMR playback
     *                      were to occur.
     */
    R compute(Object[] accessArgs, Object[] mutatorArgs);

   enum DirectReadErrors {
        DIRECT_READ_FAILED
    }
}
