package org.corfudb.runtime.object;

/**
 * Created by mwei on 11/12/16.
 */
public interface ICorfuSMRProxyContainer<T> {
    /** Set the proxy. The $CORFUSMR suffix excludes this
     * method from processing.
     * @param proxy The proxy to use for the container methods.
     */
    void setProxy$CORFUSMR(ICorfuSMRProxy<T> proxy);
}
