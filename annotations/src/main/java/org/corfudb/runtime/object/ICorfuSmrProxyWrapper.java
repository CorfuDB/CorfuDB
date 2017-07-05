package org.corfudb.runtime.object;

/** The interface to a Corfu wrapper. A wrapper object
 * passes calls to a proxy, which invokes methods on behalf of the wrapper.
 * This interface allows the proxy to be set.
 *
 * <p>Created by mwei on 11/12/16.
 *
 * @param <T> The type of the SMR object.
 */
public interface ICorfuSmrProxyWrapper<T> {
    /** Set the proxy. The $CORFUSMR suffix excludes this
     * method from processing.
     * @param proxy The proxy to use for the wrapper methods.
     */
    // Suppress method name $CORFUSMR, which is used internally.
    @SuppressWarnings({"checkstyle:methodname", "checkstyle:abbreviation"})
    void setProxy$CORFUSMR(ICorfuSmrProxy<T> proxy);
}
