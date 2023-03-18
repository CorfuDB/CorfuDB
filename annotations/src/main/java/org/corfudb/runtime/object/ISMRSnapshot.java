package org.corfudb.runtime.object;

/**
 * Interface definition for snapshots stored in the MVOCache.
 * @param <T>
 */
public interface ISMRSnapshot<T> {

    /**
     * Consume this snapshot. In some cases, the implementation
     * might want to transform or do additional processing
     * when a snapshot is consumed.
     *
     * @return T
     */
    T consume();



    /**
     * Release this snapshot.
     */
    void release();

}
