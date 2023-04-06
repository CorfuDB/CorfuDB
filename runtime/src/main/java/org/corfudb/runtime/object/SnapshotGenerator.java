package org.corfudb.runtime.object;

import org.corfudb.runtime.view.ObjectOpenOption;

import java.util.Optional;

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
    SMRSnapshot<T> generateSnapshot(VersionedObjectIdentifier version);

    Optional<SMRSnapshot<T>> generateTargetSnapshot(
            VersionedObjectIdentifier version,
            ObjectOpenOption objectOpenOption,
            SMRSnapshot<T> previousSnapshot);

    Optional<SMRSnapshot<T>> generateIntermediarySnapshot(
            VersionedObjectIdentifier version,
            ObjectOpenOption objectOpenOption);

    /**
     * {@inheritDoc}
     */
    @Override
    void close();
}
