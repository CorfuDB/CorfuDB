package org.corfudb.util.concurrent;

import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

/**
 * Utility class which implements a singleton resource pattern.
 *
 * <p>A {@link SingletonResource} is a common resource pattern where the first thread that
 * needs to use a resource instantiates it. Subsequent threads should re-use the resource
 * instantiated by the first thread.
 *
 * @param <T> The type of resource this {@link SingletonResource} holds.
 */
public class SingletonResource<T> {

    /**
     * A generator which provides the resource.
     */
    private final Supplier<T> generator;

    /**
     * The resource to be held.
     */
    private volatile T resource;

    /**
     * Factory method with similar semantics as a {@link ThreadLocal}.
     *
     * @param generator A method to be called when a new {@link R} is needed.
     * @param <R>       The type of the resource to be provided.
     * @return A new {@link SingletonResource}.
     */
    public static <R> SingletonResource<R> withInitial(@Nonnull Supplier<R> generator) {
        return new SingletonResource<>(generator);
    }

    /**
     * Generate a new {@link SingletonResource}.
     *
     * @param generator A method to be called when a new {@link T} is needed.
     */
    private SingletonResource(Supplier<T> generator) {
        this.generator = generator;

    }

    /**
     * Get the resource, potentially generating it by calling the {@code generator} if necessary.
     *
     * @return The resource provided by this {@link SingletonResource}.
     */
    public T get() {
        T temp = resource;
        if (temp == null) {
            synchronized (this) {
                temp = resource;
                if (temp == null) {
                    temp = generator.get();
                    resource = temp;
                }
            }
        }
        return temp;
    }

    /**
     * Note(Maithem): We need to remove this method, it shouldn't be used. This singleton
     * class is misused by other classes that depend on this method. To elaborate, the
     * ManagementServer class uses the lazy evaluation property of the singleton to
     * create a runtime, but not connect at construction time (to avoid blocking).
     *
     * Removing this method would mean, callers would have to get the reference to the
     * object and call clean up directly, instead of calling cleanup on the singleton.
     * For some tests, this would block because in singleton.get().cleanup would cause
     * a creation of a CorfuRuntime and connect, where the connection would fail.
     *
     * Cleanup the resource if it has been generated. Otherwise does nothing.
     *
     * @param cleaner A {@link Consumer} which is provided the resource to perform cleanup
     *                actions.
     */
    public synchronized void cleanup(@Nonnull Consumer<T> cleaner) {
        if (resource != null) {
            cleaner.accept(resource);
            resource = null;
        }
    }

}