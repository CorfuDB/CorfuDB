package org.corfudb.runtime.collections;

import org.corfudb.annotations.Accessor;

/**
 * Created by mwei on 11/12/16.
 */
@Deprecated // TODO: Add replacement method that conforms to style
@SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
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