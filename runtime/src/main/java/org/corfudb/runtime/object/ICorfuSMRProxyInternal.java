package org.corfudb.runtime.object;

/**
 * An internal interface to the SMR Proxy.
 *
 * <p>This interface contains methods which are used only by Corfu's object layer,
 * and shouldn't be exposed to other packages
 * (such as the annotation processor).
 *
 * <p>Created by mwei on 11/15/16.
 */
@Deprecated // TODO: Add replacement method that conforms to style
@SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
public interface ICorfuSMRProxyInternal<T> extends ICorfuSMRProxy<T> {

    /**
     * Directly get the state of the object the proxy is managing,
     * without causing a sync.
     */
    VersionLockedObject<T> getUnderlyingObject();
}
