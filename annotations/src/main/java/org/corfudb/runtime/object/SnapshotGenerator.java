package org.corfudb.runtime.object;

/**
 * Interface that facilitates creation of snapshots.
 *
 * @param <T>
 */
public interface SnapshotGenerator<T> extends AutoCloseable {

    /**
     * Generate a new snapshot of the underlying data structure
     * and associate a version with it.
     *
     * @param version       a version that will be associated
     *                      with this snapshot
     * @return ISMRSnapshot a new snapshot
     */
    SMRSnapshot<T> getSnapshot(VersionedObjectIdentifier version);

    /**
     * {@inheritDoc}
     */
    @Override
    void close();
}
