package org.corfudb.runtime.object;

/**
 * Interface definition for snapshots of SMR objects. Snapshots can be
 * consumed, which provides a view of the snapshot that has the same API as
 * the original SMR object. These views are effectively isolated from one
 * another, and optimistic updates made on a view are not reflected in the
 * original SMR object.
 *
 * @param <T> The type of the views produced by this snapshot.
 */
public interface SMRSnapshot<T> {

    /**
     * Consume this snapshot and produce a new view. In some cases, the
     * implementation might want to transform or do additional processing
     * when a snapshot is consumed.
     *
     * @return A view for this snapshot.
     */
    T consume();

    /**
     * Release resources associated with this snapshot.
     * <p>
     * WARNING: This method needs to be idempotent.
     */
    boolean release();
}
