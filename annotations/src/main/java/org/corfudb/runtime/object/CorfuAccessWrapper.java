package org.corfudb.runtime.object;

import lombok.Getter;

import java.util.function.BiFunction;


/** This wrapper class forces accesses to go through the proxy,
 * which synchronizes any accesses to the correct version.
 *
 * @param <T>   The type of the object being wrapped.
 * @param <P>   The type of the proxy being used.
 * Created by mwei on 5/18/17.
 */
public class CorfuAccessWrapper<T, P> {

    /** The object which is being wrapped. */
    @Getter
    private final T object;

    /** The proxy to synchronize accesses against. */
    @Getter
    private final ICorfuSMRProxy<P> proxy;

    /** Generate a new wrapper.
     *
     * @param wrappedObject     The object to wrap.
     * @param parentProxy             The proxy to use.
     */
    public CorfuAccessWrapper(final T wrappedObject,
                              final ICorfuSMRProxy<P> parentProxy) {
        this.object = wrappedObject;
        this.proxy = parentProxy;
    }

    /** Perform a wrapped access, which executes the given function
     * under the correct version.
     * @param wrapFunction      The function to wrap.
     * @param conflictObjects   The set of conflict objects.
     * @param <R>               The return type of the wrapFunction.
     * @return                  The return value calculated by the
     *                          wrap function.
     */
    public <R> R wrappedAccess(final BiFunction<T, P, R> wrapFunction,
                               final Object[] conflictObjects) {
        return proxy.access(o -> wrapFunction.apply(object, o),
                conflictObjects);
    }
}
