package org.corfudb.runtime.object;

/**
 * Sub type.
 *
 * @param <T> ty0pe
 */
public interface SnapshotGenerator<T> extends AutoCloseable {
    /**
     *
     * @param version version
     * @return ISMRSnapshot
     */
    ISMRSnapshot<T> getSnapshot(VersionedObjectIdentifier version);

    @Override
    void close();
}
