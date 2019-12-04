package org.corfudb.runtime.collections;

import org.corfudb.runtime.object.ICorfuExecutionContext;

/**
 * A flavour of {@link StreamingMap} that is {@link ICorfuExecutionContext} aware
 * and {@link AutoCloseable}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface ContextAwareMap<K, V> extends StreamingMap<K, V>, AutoCloseable {

    /**
     * Return an optional implementation of the {@link StreamingMap} that
     * is used only during optimistic (non-committed) operations.
     *
     * It is the responsibility of the data-structure to query this map during
     * any sort of access operations.
     *
     * @return {@link StreamingMap} representing non-committed changes
     */
    default ContextAwareMap<K, V> getOptimisticMap() {
        return this;
    }

    /**
     * Relinquish any resources associated with this object.
     */
    default void close() {
    }
}
