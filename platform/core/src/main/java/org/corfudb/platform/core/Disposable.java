package org.corfudb.platform.core;

import java.util.concurrent.CompletableFuture;

/**
 * Denote an entity or resource that may be cancelled or disposed via {@link #dispose()}, which
 * should be an idempotent operation.
 *
 * @author jameschang
 * @since 2018-07-25
 */
@FunctionalInterface
public interface Disposable extends AutoCloseable {

    @Override
    default void close() throws RuntimeException {
        dispose().join();
    }

    /**
     * Cancel or dispose this resource.
     * <p>
     * Cancellation may engender background processing that interested entities may then await for
     * completion. Concrete implementation that allow idempotent disposal operations should allow
     * awaiting invocations to await on the same underlying background task completion.
     *
     * @return    {@code true} if resource is definitely cancelled, {@code false} otherwise.
     */
    CompletableFuture<Boolean> dispose();
}
