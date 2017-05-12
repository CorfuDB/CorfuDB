package org.corfudb.runtime.object;

/** An internal interface to the SMR Proxy.
 *
 * This interface contains methods which are used only by Corfu's object layer,
 * and shouldn't be exposed to other packages
 * (such as the annotation processor).
 *
 * Created by mwei on 11/15/16.
 */
public interface ICorfuSMRProxyInternal<T> extends ICorfuSMRProxy<T> {

    /** Directly get the state of the object the proxy is managing,
     * without causing a sync. */
    VersionLockedObject<T> getUnderlyingObject();
}
