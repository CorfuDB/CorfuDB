package org.corfudb.util.concurrent;

import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

/** Utility class which implements a singleton resource pattern.
 *
 *  <p>A {@link SingletonResource} is a common resource pattern where the first thread that
 *  needs to use a resource instantiates it. Subsequent threads should re-use the resource
 *  instantiated by the first thread. The {@link SingletonResource#cleanup(Consumer)} allows
 *  the developer to release resources if the resource has been instantiated (doing nothing
 *  if no thread ever created the resource in the first place).
 *
 * @param <T>   The type of resource this {@link SingletonResource} holds.
 */
public class SingletonResource<T> {

    /** The resource to be held. */
    private T resource;

    /** A generator which provides the resource. */
    private final Supplier<T> generator;

    /** A stamped lock which controls access to the resource. */
    private final StampedLock lock;

    /** Factory method with similar semantics as a {@link ThreadLocal}.
     *
     * @param generator     A method to be called when a new {@link R} is needed.
     * @param <R>           The type of the resource to be provided.
     * @return              A new {@link SingletonResource}.
     */
    public static <R> SingletonResource<R> withInitial(@Nonnull Supplier<R> generator) {
        return new SingletonResource<>(generator);
    }

    /** Generate a new {@link SingletonResource}.
     *
     * @param generator     A method to be called when a new {@link T} is needed.
     */
    private SingletonResource(Supplier<T> generator) {
        lock = new StampedLock();
        this.generator = generator;

    }

    /** Get the resource, potentially generating it by calling the {@code generator} if necessary.
     *
     * @return  The resource provided by this {@link SingletonResource}.
     */
    public T get() {
        long ts = lock.tryOptimisticRead();
        if (ts != 0 && resource != null && lock.validate(ts)) {
            // If the optimistic read succeeds and the resource
            // is present, return it.
            return resource;
        } else {
            // Otherwise, grab a read lock (this should be rare)
            try {
                ts = lock.readLock();
                // The resource was present, so return it.
                if (resource != null) {
                    return resource;
                } else {
                    // Otherwise attempt to grab a write lock so we can initialize
                    // the resource.
                    ts = lock.tryConvertToWriteLock(ts);
                    // Upgrading the read lock failed, so unlock and relock
                    if (ts == 0) {
                        lock.unlock(ts);
                        ts = lock.writeLock();
                    }
                    resource = generator.get();
                    return resource;
                }
            } finally {
                lock.unlock(ts);
            }
        }
    }

    /** Cleanup the resource if it has been generated. Otherwise does nothing.
     *
     * @param cleaner   A {@link Consumer} which is provided the resource to perform cleanup
     *                  actions.
     */
    public void cleanup(@Nonnull Consumer<T> cleaner) {
        long ts = lock.writeLock();
        try {
            if (resource != null) {
                // Perform cleanup actions and release the resource
                cleaner.accept(resource);
            }
            resource = null;
        } finally {
            lock.unlock(ts);
        }
    }

}