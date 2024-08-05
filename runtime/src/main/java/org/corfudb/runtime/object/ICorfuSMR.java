package org.corfudb.runtime.object;

import java.util.Map;

/**
 * The interface for an object interfaced with SMR.
 *
 * @param <S> that extends {@link SnapshotGenerator} and {@link ConsistencyView}
 */
public interface ICorfuSMR<S extends SnapshotGenerator<S>> extends AutoCloseable {

    /** Get the proxy for this wrapper, to manage the state of the object.
     * @return The proxy for this wrapper. */
    ICorfuSMRProxy<?> getCorfuSMRProxy();

    /**
     * Set the proxy for this wrapper, to manage the state of the object.
     * @param proxy The proxy to set for this wrapper.
     */
    void setCorfuSMRProxy(ICorfuSMRProxy<S> proxy);

    /**
     * Get a map from strings (function names) to SMR upcalls.
     * @param <R> The return type for ICorfuSMRUpcallTargets
     * @return A map from function names to SMR upcalls
     */
    <R> Map<String, ICorfuSMRUpcallTarget<R>> getSMRUpcallMap();
}
