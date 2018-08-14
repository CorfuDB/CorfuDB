package org.corfudb.util.concurrent;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

/**
 * Utility class which implements a singleton resource pattern.
 *
 * <p>A {@link SingletonResource} is a common resource pattern where the first thread that
 * needs to use a resource instantiates it. Subsequent threads should re-use the resource
 * instantiated by the first thread. The {@link SingletonResource#cleanup(Consumer)} allows
 * the developer to release resources if the resource has been instantiated (doing nothing
 * if no thread ever created the resource in the first place).
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
    private final AtomicReference<T> resource;

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
        this.resource = new AtomicReference<T>();
        this.generator = generator;

    }

    /**
     * Get the resource, potentially generating it by calling the {@code generator} if necessary.
     *
     * @return The resource provided by this {@link SingletonResource}.
     */
    public T get() {
        return resource.updateAndGet(t -> t == null ? generator.get() : t);
    }

    /**
     * Cleanup the resource if it has been generated. Otherwise does nothing.
     *
     * @param cleaner A {@link Consumer} which is provided the resource to perform cleanup
     *                actions.
     */
    public void cleanup(@Nonnull Consumer<T> cleaner) {
        resource.updateAndGet(t -> {
            if (t != null) {
                cleaner.accept(t);
            }
            return null;
        });
    }

}