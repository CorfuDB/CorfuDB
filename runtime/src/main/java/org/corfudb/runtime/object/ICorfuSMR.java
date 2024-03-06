package org.corfudb.runtime.object;

import org.corfudb.runtime.object.SnapshotGenerator.SnapshotGeneratorWithConsistency;

import java.util.Map;

/**
 * The interface for an object interfaced with SMR.
 *
 * @param <S> that extends {@link SnapshotGenerator} and {@link ConsistencyView}
 */
public interface ICorfuSMR<S extends SnapshotGeneratorWithConsistency<S>> extends AutoCloseable {

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
     * @return A map from function names to SMR upcalls
     */
    Map<String, CorfuSmrUpcallTarget<S>> getSmrUpCallMap();
}
