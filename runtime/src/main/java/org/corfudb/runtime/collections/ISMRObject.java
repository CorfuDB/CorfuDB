package org.corfudb.runtime.collections;

import org.corfudb.annotations.Accessor;

import java.util.HashMap;

/**
 * Created by mwei on 11/12/16.
 */
public interface ISMRObject {

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    int hashCode();

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    boolean equals(Object obj);

    /**
     * {@inheritDoc}
     */
    @Accessor
    String toString();
}