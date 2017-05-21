package org.corfudb.runtime.object;

import java.util.function.BiFunction;


/** This wrapper class forces accesses to go through the proxy,
 * which synchronizes any accesses to the correct version.
 *
 * @param <T>   The type of the object being wrapped.
 * @param <P>   The type of the proxy being used.
 * Created by mwei on 5/18/17.
 */
public interface ICorfuAccessWrapper<T, P> {

    /** Get the object which is being wrapped.
     * @return The object that was wrapped.
     * */
    T getObject$CORFUSMR();

    /** Get a proxy to synchronize accesses against.
     * @return  The proxy this wrapped object came from. */
    ICorfuSMRProxy<P> getProxy$CORFUSMR();

    /** Perform a wrapped access, which executes the given function
     * under the correct version.
     * @param wrapFunction      The function to wrap.
     * @param conflictObjects   The set of conflict objects.
     * @param <R>               The return type of the wrapFunction.
     * @return                  The return value calculated by the
     *                          wrap function.
     */
    default <R> R wrappedAccess$CORFUSMR(
            final BiFunction<T, P, R> wrapFunction,
                               final Object[] conflictObjects) {
        return getProxy$CORFUSMR().access(o ->
                        wrapFunction.apply(getObject$CORFUSMR(), o),
                conflictObjects, null);
    }
}
