package org.corfudb.runtime.object;

/** The interface to a Corfu wrapper. A wrapper object
 * passes calls to a proxy, which invokes methods on behalf of the wrapper.
 * This interface allows the proxy to be set.
 * @param <T> The type of the SMR object.
 * Created by mwei on 11/12/16.
 */
public interface ICorfuSMRProxyWrapper<T> {
    /** Set the proxy. The $CORFUSMR suffix excludes this
     * method from processing.
     * @param proxy The proxy to use for the wrapper methods.
     */
    void setProxy$CORFUSMR(ICorfuSMRProxy<T> proxy);
}
