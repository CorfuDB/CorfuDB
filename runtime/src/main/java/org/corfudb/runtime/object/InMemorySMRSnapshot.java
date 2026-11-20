package org.corfudb.runtime.object;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * An in-memory implementation of a snapshot.
 * Views produced via consume() are immutable.
 *
 * @param <T> The type of the views produced by this snapshot.
 */
@AllArgsConstructor
public class InMemorySMRSnapshot<T> implements SMRSnapshot<T> {
    private final T snapshot;

    @Getter
    private final VersionedObjectStats metrics = new VersionedObjectStats();

    /**
     * {@inheritDoc}
     */
    @Override
    public T consume() {
        return snapshot;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void release() {
        // No-Op.
    }
}
