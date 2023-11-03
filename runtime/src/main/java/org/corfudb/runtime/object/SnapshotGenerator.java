package org.corfudb.runtime.object;

import org.corfudb.runtime.view.ObjectOpenOption;

import java.util.Optional;

/**
 * Interface that facilitates creation of snapshots.
 *
 * @param <T>
 */
public interface SnapshotGenerator<T> extends AutoCloseable {

    interface SnapshotGeneratorWithConsistency<S> extends SnapshotGenerator<S>, ConsistencyView {

    }

    /**
     * Generate a new snapshot of the underlying data structure
     * and associate a version with it.
     *
     * @param version       a version that will be associated
     *                      with this snapshot
     * @return ISMRSnapshot a new snapshot
     */
    SMRSnapshot<T> generateSnapshot(VersionedObjectIdentifier version);

    /**
     * Create a target snapshot associated with the provided version.
     * If a client is at version N, and it wants to access version M,
     * then the target snapshot is M.
     *
     * @param version version associated with the intermediary snapshot
     * @param objectOpenOption how this object was opened
     * @param previousSnapshot previously generated snapshot
     * @return a new snapshot associated with the provided version.
     */
    Optional<SMRSnapshot<T>> generateTargetSnapshot(
            VersionedObjectIdentifier version,
            ObjectOpenOption objectOpenOption,
            SMRSnapshot<T> previousSnapshot);

    /**
     * Create an intermediary snapshot associated with the provided version.
     * If a client is at version N, and it wants to access version M,
     * then the intermediary snapshots are the ones between N and M (exclusive).
     *
     * @param version version associated with the intermediary snapshot
     * @param objectOpenOption how this object was opened
     * @return a new snapshot associated with the provided version.
     */
    Optional<SMRSnapshot<T>> generateIntermediarySnapshot(
            VersionedObjectIdentifier version,
            ObjectOpenOption objectOpenOption);

    /**
     * {@inheritDoc}
     */
    @Override
    void close();
}
