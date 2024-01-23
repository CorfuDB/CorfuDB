package org.corfudb.runtime.object;

import lombok.AllArgsConstructor;

/**
 * An in-memory implementation of a snapshot.
 * Views produced via consume() are immutable.
 *
 * @param <T> The type of the views produced by this snapshot.
 */
@AllArgsConstructor
public class InMemorySMRSnapshot<T> implements SMRSnapshot<T> {
    private final T snapshot;

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
    public boolean release() {
        // No-Op.
        return true;
    }
}
