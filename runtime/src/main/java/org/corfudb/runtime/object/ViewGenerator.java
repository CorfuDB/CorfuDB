package org.corfudb.runtime.object;

import lombok.NonNull;

/**
 * Interface that facilitates creation of isolated views.
 *
 * @param <S> which needs to extend SnapshotGenerator
 */
public interface ViewGenerator<S extends SnapshotGenerator<S>> {

    /**
     * Create a new isolated view given the underlying
     * {@link RocksDbApi} object.
     *
     * @param rocksApi object that the view is going to be based on
     * @return         a new view
     */
    S newView(@NonNull RocksDbApi<S> rocksApi);
}
