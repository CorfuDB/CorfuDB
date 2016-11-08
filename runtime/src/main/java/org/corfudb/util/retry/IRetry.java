package org.corfudb.util.retry;

import lombok.SneakyThrows;

import java.util.Map;
import java.util.function.Consumer;

/**
 * The IRetry interface is used to support implementing retry loops.
 * Special exceptions can be handled by registering a handler, which
 * returns false if the loop should be aborted.
 * <p>
 * Created by mwei on 9/1/15.
 */
public interface IRetry<E extends Exception, F extends Exception, G extends Exception, H extends Exception, O, A extends IRetry> {

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <R, Z extends IRetry> IRetry<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R, Z> build(Class<Z> retryType, IRetryable<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R> runFunction) {
        return (IRetry<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R, Z>) retryType.getConstructor(IRetryable.class).newInstance(runFunction);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, R, Z extends IRetry> IRetry<T, RuntimeException, RuntimeException, RuntimeException, R, Z>
    build(Class<Z> retryType, Class<? extends T> firstExceptionType, IRetryable<T, RuntimeException, RuntimeException, RuntimeException, R> runFunction) {
        return (IRetry<T, RuntimeException, RuntimeException, RuntimeException, R, Z>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType);
    }
    /* The following are builders which are basically a hack to support varidic exceptions... */

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, R, Z extends IRetry> IRetry<T, U, RuntimeException, RuntimeException, R, Z>
    build(Class<Z> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType, IRetryable<T, U, RuntimeException, RuntimeException, R> runFunction) {
        return (IRetry<T, U, RuntimeException, RuntimeException, R, Z>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, V extends Exception, R, Z extends IRetry> IRetry<T, U, V, RuntimeException, R, Z>
    build(Class<Z> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType, Class<? extends V> thirdExceptionType, IRetryable<T, U, V, RuntimeException, R> runFunction) {
        return (IRetry<T, U, V, RuntimeException, R, Z>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType)
                .passException(thirdExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, V extends Exception, W extends Exception, R, Z extends IRetry>
    IRetry<T, U, V, W, R, Z> build(Class<Z> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType,
                                   Class<? extends V> thirdExceptionType, Class<? extends W> fourthExceptionType, IRetryable<T, U, V, W, R> runFunction) {
        return (IRetry<T, U, V, W, R, Z>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType)
                .passException(thirdExceptionType)
                .passException(fourthExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <R, Z extends IRetry> IRetry<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R, Z> buildStrict(Class<Z> retryType, IStrictRetryable<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R> runFunction) {
        return (IRetry<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R, Z>) retryType.getConstructor(IRetryable.class).newInstance(runFunction);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, R, Z extends IRetry> IRetry<T, RuntimeException, RuntimeException, RuntimeException, R, Z>
    buildStrict(Class<Z> retryType, Class<? extends T> firstExceptionType, IStrictRetryable<T, RuntimeException, RuntimeException, RuntimeException, R> runFunction) {
        return (IRetry<T, RuntimeException, RuntimeException, RuntimeException, R, Z>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, R, Z extends IRetry> IRetry<T, U, RuntimeException, RuntimeException, R, Z>
    buildStrict(Class<Z> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType, IStrictRetryable<T, U, RuntimeException, RuntimeException, R> runFunction) {
        return (IRetry<T, U, RuntimeException, RuntimeException, R, Z>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, V extends Exception, R, Z extends IRetry> IRetry<T, U, V, RuntimeException, R, Z>
    buildStrict(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType, Class<? extends V> thirdExceptionType, IStrictRetryable<T, U, V, RuntimeException, R> runFunction) {
        return (IRetry<T, U, V, RuntimeException, R, Z>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType)
                .passException(thirdExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, V extends Exception, W extends Exception, R, Z extends IRetry>
    IRetry<T, U, V, W, R, Z> buildStrict(Class<Z> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType,
                                         Class<? extends V> thirdExceptionType, Class<? extends W> fourthExceptionType, IStrictRetryable<T, U, V, W, R> runFunction) {
        return (IRetry<T, U, V, W, R, Z>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType)
                .passException(thirdExceptionType)
                .passException(fourthExceptionType);
    }

    /**
     * Run the retry. This function may never return.
     */
    @SuppressWarnings("unchecked")
    default O run()
            throws E, F, G, H {
        boolean retry = true;
        while (retry) {
            try {
                return getRunFunction().retryFunction();
            } catch (Exception ex) {
                if (getHandlerMap().containsKey(ex.getClass())) {
                    retry = ((ExceptionHandler<Exception, E, F, G, H, A>) getHandlerMap().get(ex.getClass())).HandleException(ex, (A) this) && handleException(ex, false);
                } else {
                    retry = handleException(ex, true);
                }
            }
        }
        return null;
    }

    /**
     * Run the retry forever, unless stopped by the retryer. This function may never return.
     */
    @SuppressWarnings("unchecked")
    default void runForever()
            throws E, F, G, H {
        boolean retry = true;
        while (retry) {
            try {
                getRunFunction().retryFunction();
                retry = retryLogic();
            } catch (Exception ex) {
                if (getHandlerMap().containsKey(ex.getClass())) {
                    retry = ((ExceptionHandler<Exception, E, F, G, H, A>) getHandlerMap().get(ex.getClass())).HandleException(ex, (A) this) && handleException(ex, false);
                } else {
                    retry = handleException(ex, true);
                }
            }
        }
    }

    /**
     * Get the function that needs to be retried.
     */
    IRetryable<E, F, G, H, O> getRunFunction();

    /**
     * Handle an exception which has occurred.
     *
     * @param e         The exception that has occurred.
     * @param unhandled If the exception is unhandled.
     * @return True, to continue retrying, or False, to stop running the function.
     */
    boolean handleException(Exception e, boolean unhandled);

    /**
     * Get the exception handler map for the retry logic.
     *
     * @return The exception handler map for the retry logic.
     */
    Map<Class<? extends Exception>, ExceptionHandler> getHandlerMap();

    /**
     * Pass an exception by throwing it to the main body.
     *
     * @param exceptionType The exception to pass to the main body.
     * @param <T>           The type of exceptions to pass.
     * @return An IRetry, to support a fluent API interface.
     */
    default <T extends Exception> IRetry<E, F, G, H, O, A> passException(Class<T> exceptionType) {
        getHandlerMap().put(exceptionType, (e, x) -> {
            throw e;
        });
        return this;
    }

    /**
     * Register an exception handler when an exception occurs.
     * Note that only one exception handler can be registered per exception type.
     *
     * @param exceptionType The type of exception to handle.
     * @param handler       The action to take when an exception occurs. The handler should return true
     *                      if the retry logic should continue, or false if the logic should abort.
     * @return An IRetry, to support a fluent API interface.
     */
    @SuppressWarnings("unchecked")
    default <T extends Exception> IRetry<E, F, G, H, O, A> onException(Class<T> exceptionType, ExceptionHandler<T, E, F, G, H, A> handler) {
        getHandlerMap().put(exceptionType, (ExceptionHandler) handler);
        return this;
    }

    /**
     * Register an exception handler when an exception occurs.
     * Note that only one exception handler can be registered per exception type.
     *
     * @param exceptionType The type of exception to handle.
     * @param handler       The action to take when an exception occurs. The handler should return true
     *                      if the retry logic should continue, or false if the logic should abort.
     * @return An IRetry, to support a fluent API interface.
     */
    @SuppressWarnings("unchecked")
    default <T extends Exception> IRetry<E, F, G, H, O, A> onException(Class<T> exceptionType, ExceptionHandlerWithoutRetry<T, E, F, G, H> handler) {
        getHandlerMap().put(exceptionType, (e, r) -> handler.HandleException((T) e));
        return this;
    }

    /**
     * Configure settings for the underlying class.
     * The consumer will be given access to the underlying retry type to configure type-specific options.
     *
     * @param settingsFunction A consumer with access to the underlying retry type.
     * @return An IRetry, to support a fluent API interface.
     */
    @SuppressWarnings("unchecked")
    default IRetry<E, F, G, H, O, A> setOptions(Consumer<A> settingsFunction) {
        settingsFunction.accept((A) this);
        return this;
    }

    /**
     * Apply the retry logic.
     *
     * @return True, if we should continue retrying, false otherwise.
     */
    boolean retryLogic();

    /**
     * This is a functional interface for handling exceptions. It gets the exception to handle as
     * a parameter and should return true if the retry logic should continue, false otherwise.
     */
    @FunctionalInterface
    interface ExceptionHandler<T extends Exception, U extends Exception, V extends Exception, W extends Exception, X extends Exception, Z extends IRetry> {
        boolean HandleException(T e, Z retry) throws U, V, W, X;
    }

    /**
     * This is a functional interface for handling exceptions. It gets the exception to handle as
     * a parameter and should return true if the retry logic should continue, false otherwise.
     */
    @FunctionalInterface
    interface ExceptionHandlerWithoutRetry<T extends Exception, U extends Exception, V extends Exception, W extends Exception, X extends Exception> {
        boolean HandleException(T e) throws U, V, W, X;
    }
}
