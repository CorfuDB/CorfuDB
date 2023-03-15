package org.corfudb.runtime.object;

import java.util.Map;

/** The interface for an object interfaced with SMR.
 * @param <T> The type of the underlying object.
 * Created by mwei on 11/10/16.
 */
public interface ICorfuSMR extends AutoCloseable {

    /** Get the proxy for this wrapper, to manage the state of the object.
     * @return The proxy for this wrapper. */
    ICorfuSMRProxy<?> getCorfuSMRProxy();

    /**
     * Set the proxy for this wrapper, to manage the state of the object.
     * @param proxy The proxy to set for this wrapper.
     * @param <R> The type used for managing underlying versions.
     */
    <R> void setCorfuSMRProxy(ICorfuSMRProxy<R> proxy);

    /**
     * Get a map from strings (function names) to SMR upcalls.
     * @param <R> The return type for ICorfuSMRUpcallTargets
     * @return A map from function names to SMR upcalls
     */
    <R> Map<String, ICorfuSMRUpcallTarget<R>> getSMRUpcallMap();
}
