package org.corfudb.platform.core;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Container type for encapsulating function result and propagating errors.
 *
 * @author jameschang
 * @since 2018-07-25
 */
public class Result<T, E extends RuntimeException> implements Supplier<T> {

    private final T value;
    private final E error;

    public Result(T value) {
        this(Objects.requireNonNull(value), null);
    }

    public Result(E error) {
        this(null, Objects.requireNonNull(error));
    }

    private Result(T value, E error) {
        // Exactly one value/error is non-null based on constructors calling this constructor.
        this.value = value;
        this.error = error;
    }

    /**
     * Returns the encapsulated result if the {@link Result} instance is not an error.
     *
     * @return the encapsulated {@link #value}.
     * @throws E if instance encapsulates an error.
     */
    @Override
    public T get() {
        if (error != null) {
            throw error;
        }

        return value;
    }

    /**
     * Returns the encapsulated error if the {@link Result} instance is an error.
     *
     * @return the encapsulated {@link #error}.
     * @throws NoSuchElementException if there is no error present.
     */
    public E getError() {
        if (error == null) {
            throw new NoSuchElementException("No error present");
        }

        return error;
    }

    /**
     * Returns boolean indicating whether the {@link Result} instance has a valid result value.
     *
     * @return {@code true} if instance contains valid result, {@code false} otherwise.
     */
    public boolean isPresent() {
        return (value != null);
    }

    /**
     * Returns the encapsulated result if the {@link Result} instance is not an error, otherwise
     * return the supplied alternative value.
     *
     * @param other alternative value to return if instance encapsulates an error.
     * @return the encapsulated value if present, otherwise {@code other}
     */
    public T orElse(T other) {
        return isPresent() ? value : other;
    }

    /**
     * Performs the given {@link Consumer} action with the value if the {@link Result} instance is
     * a valid result value, otherwise perform the given alternative action.
     *
     * @param consumer action to perform if instance encapsulates a valid result.
     * @param action   action to perform if instance encapsulates an empty value or an error.
     */
    public void ifPresentOrElse(Consumer<T> consumer, Runnable action) {
        if (isPresent()) {
            consumer.accept(value);
        } else {
            action.run();
        }
    }

    /**
     * Maps a given function to the {@link Result#value}, if the result value is valid.
     *
     * @param function mapping function to apply to the internal result
     * @param <U>      type of the mapped value
     * @return a new instance of {@link Result} which contains the result of applying the mapping
     * function to the original internal value.
     */
    public <U> Result<U, E> map(Function<? super T, ? extends U> function) {
        if (isPresent()) {
            return new Result<>(function.apply(value), error);
        } else {
            return new Result<>(null, error);
        }
    }
}
