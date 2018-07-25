package org.corfudb.util;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Provides a utility to throw checked Exceptions from lambdas.
 */
public final class CorfuLambda {
    @FunctionalInterface
    public interface FunctionWithExceptions<T, R, E extends Exception> {
        R apply(T t) throws E;
    }

    public static <T, R, E extends Exception> Function<T, R> function(FunctionWithExceptions<T, R, E> function) throws E {
        return t -> {
            try {
                return function.apply(t);
            } catch (Exception exception) {
                throwAsUnchecked(exception);
                return null;
            }
        };
    }

    @FunctionalInterface
    public interface ConsumerWithExceptions<T, E extends Exception> {
        void accept(T t) throws E;
    }

    /**
     * Consumer Utility to throw checked Exceptions.
     *
     * @param consumer Consumer Lambda.
     * @param <T>      Consumer Parameter.
     * @param <E>      Exception thrown by the consumer.
     * @return Returns the result of the consumer.
     * @throws E Throws Exception if any.
     */
    public static <T, E extends Exception> Consumer<T> consumer(ConsumerWithExceptions<T, E> consumer) throws E {
        return t -> {
            try {
                consumer.accept(t);
            } catch (Exception exception) {
                throwAsUnchecked(exception);
            }
        };
    }

    @FunctionalInterface
    public interface BiConsumerWithExceptions<T, U, E extends Exception> {
        void accept(T t, U u) throws E;
    }

    public static <T, U, E extends Exception> BiConsumer<T, U> biConsumer(BiConsumerWithExceptions<T, U, E> biConsumer) throws E {
        return (t, u) -> {
            try {
                biConsumer.accept(t, u);
            } catch (Exception exception) {
                throwAsUnchecked(exception);
            }
        };
    }

    @FunctionalInterface
    public interface SupplierWithExceptions<T, E extends Exception> {
        T get() throws E;
    }

    public static <T, E extends Exception> Supplier<T> supplier(SupplierWithExceptions<T, E> function) throws E {
        return () -> {
            try {
                return function.get();
            } catch (Exception exception) {
                throwAsUnchecked(exception);
                return null;
            }
        };
    }

    @FunctionalInterface
    public interface RunnableWithExceptions<E extends Exception> {
        void run() throws E;

    }

    public static void uncheck(RunnableWithExceptions t) {
        try {
            t.run();
        } catch (Exception exception) {
            throwAsUnchecked(exception);
        }
    }

    public static <R, E extends Exception> R uncheck(SupplierWithExceptions<R, E> supplier) {
        try {
            return supplier.get();
        } catch (Exception exception) {
            throwAsUnchecked(exception);
            return null;
        }
    }

    public static <T, R, E extends Exception> R uncheck(FunctionWithExceptions<T, R, E> function, T t) {
        try {
            return function.apply(t);
        } catch (Exception exception) {
            throwAsUnchecked(exception);
            return null;
        }
    }

    private static <E extends Throwable> void throwAsUnchecked(Exception exception) throws E {
        throw (E) exception;
    }

}