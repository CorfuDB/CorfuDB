package org.corfudb.runtime.object;

import lombok.NonNull;

public interface RocksDbSnapshotGenerator<S extends SnapshotGenerator<S>> {

    /**
     * Returns a snapshot associated with the provided version.
     *
     * @param viewGenerator an object that generates higher level views
     *                      based on this snapshot
     * @param version version associated with this snapshot
     * @return a snapshot associated with the provided version
     */
    SMRSnapshot<S> getSnapshot(@NonNull ViewGenerator<S> viewGenerator,
                               @NonNull VersionedObjectIdentifier version);

    /**
     * Returns an implicit read-committed snapshot.
     *
     * @param viewGenerator an object that generates higher level views
     *                      based on this snapshot
     * @return an implicit read-committed snapshot
     */
    SMRSnapshot<S> getImplicitSnapshot(
            @NonNull ViewGenerator<S> viewGenerator);
}
