package org.corfudb.runtime.object;

/**
 * Interface definition for snapshots stored in the MVOCache.
 * @param <T>
 */
public interface ISMRSnapshot<T> {

    /**
     *
     * @return T
     */
    T consume();


    /**
     *
     */
    void release();

}
