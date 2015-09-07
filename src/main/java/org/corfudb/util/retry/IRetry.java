package org.corfudb.util.retry;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * The IRetry interface is used to support implementing retry loops.
 * Special exceptions can be handled by registering a handler, which
 * returns false if the loop should be aborted.
 *
 * Created by mwei on 9/1/15.
 */
public interface IRetry<E extends Exception, F extends Exception, G extends Exception, H extends Exception, O> {

    /** Run the retry. This function may never return. */
    @SuppressWarnings("unchecked")
    default O run()
    throws E, F, G, H
    {
        boolean retry = true;
        while (retry) {
            try {
                return getRunFunction().retryFunction();
            }
            catch (Exception ex) {
                if (getHandlerMap().containsKey(ex.getClass()))
                {
                    retry = ((ExceptionHandler<Exception, E, F, G ,H>)getHandlerMap().get(ex.getClass())).HandleException(ex) && handleException(ex, false);
                }
                else {
                    retry = handleException(ex, true);
                }
            }
        }
        return null;
    }

    /* The following are builders which are basically a hack to support varidic exceptions... */

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <R> IRetry<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R> build(Class<? extends IRetry> retryType, IRetryable<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R> runFunction)
    {
        return (IRetry<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R>) retryType.getConstructor(IRetryable.class).newInstance(runFunction);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, R> IRetry<T, RuntimeException, RuntimeException, RuntimeException, R>
    build(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, IRetryable<T, RuntimeException, RuntimeException, RuntimeException, R> runFunction)
    {
        return (IRetry<T, RuntimeException, RuntimeException, RuntimeException, R>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                                                                                    .passException(firstExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, R> IRetry<T, U, RuntimeException, RuntimeException, R>
    build(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType, IRetryable<T, U, RuntimeException, RuntimeException, R> runFunction)
    {
        return (IRetry<T,U, RuntimeException, RuntimeException, R>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, V extends Exception,R> IRetry<T, U, V, RuntimeException, R>
    build(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType, Class<? extends V> thirdExceptionType, IRetryable<T, U, V, RuntimeException, R> runFunction)
    {
        return (IRetry<T,U, V, RuntimeException, R>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType)
                .passException(thirdExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, V extends Exception, W extends Exception, R>
    IRetry<T, U, V,W, R> build(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType,
                              Class<? extends V> thirdExceptionType,  Class<? extends W> fourthExceptionType,  IRetryable<T,U,V,W,R> runFunction)
    {
        return (IRetry<T,U, V, W,R>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType)
                .passException(thirdExceptionType)
                .passException(fourthExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <R> IRetry<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R> buildStrict(Class<? extends IRetry> retryType, IStrictRetryable<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R> runFunction)
    {
        return (IRetry<RuntimeException, RuntimeException, RuntimeException, RuntimeException, R>) retryType.getConstructor(IRetryable.class).newInstance(runFunction);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, R> IRetry<T, RuntimeException, RuntimeException, RuntimeException, R>
    buildStrict(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, IStrictRetryable<T, RuntimeException, RuntimeException, RuntimeException, R> runFunction)
    {
        return (IRetry<T, RuntimeException, RuntimeException, RuntimeException, R>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, R> IRetry<T, U, RuntimeException, RuntimeException, R>
    buildStrict(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType, IStrictRetryable<T, U, RuntimeException, RuntimeException, R> runFunction)
    {
        return (IRetry<T,U, RuntimeException, RuntimeException, R>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, V extends Exception,R> IRetry<T, U, V, RuntimeException, R>
    buildStrict(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType, Class<? extends V> thirdExceptionType, IStrictRetryable<T, U, V, RuntimeException, R> runFunction)
    {
        return (IRetry<T,U, V, RuntimeException, R>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType)
                .passException(thirdExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, V extends Exception, W extends Exception, R>
    IRetry<T, U, V,W, R> buildStrict(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType,
                               Class<? extends V> thirdExceptionType,  Class<? extends W> fourthExceptionType,  IStrictRetryable<T,U,V,W,R> runFunction)
    {
        return (IRetry<T,U, V, W,R>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType)
                .passException(thirdExceptionType)
                .passException(fourthExceptionType);
    }

    /** Get the function that needs to be retried. */
    IRetryable<E, F, G, H, O> getRunFunction();

    /** Handle an exception which has occurred.
     * @param e             The exception that has occurred.
     * @param unhandled     If the exception is unhandled.
     * @return              True, to continue retrying, or False, to stop running the function.
     */
    boolean handleException(Exception e, boolean unhandled);

    /**
     * This is a functional interface for handling exceptions. It gets the exception to handle as
     * a parameter and should return true if the retry logic should continue, false otherwise.
     */
    @FunctionalInterface
    interface ExceptionHandler<T extends Exception, U extends Exception, V extends Exception, W extends Exception, X extends Exception> {
        boolean HandleException(T e) throws U, V, W, X;
    }

    /**
     * Get the exception handler map for the retry logic.
     *
     * @return                  The exception handler map for the retry logic.
     */
    Map<Class<? extends Exception>, ExceptionHandler> getHandlerMap();

    /**
     * Pass an exception by throwing it to the main body.
     * @param exceptionType     The exception to pass to the main body.
     * @param <T>               The type of exceptions to pass.
     * @return                  An IRetry, to support a fluent API interface.
     */
    default <T extends Exception> IRetry<E,F,G,H,O> passException(Class<T> exceptionType)
    {
        getHandlerMap().put(exceptionType, e -> {throw e;});
        return this;
    }

    /**
     * Register an exception handler when an exception occurs.
     * Note that only one exception handler can be registered per exception type.
     * @param exceptionType     The type of exception to handle.
     * @param handler           The action to take when an exception occurs. The handler should return true
     *                          if the retry logic should continue, or false if the logic should abort.
     * @return                  An IRetry, to support a fluent API interface.
     */
    @SuppressWarnings("unchecked")
    default <T extends Exception> IRetry<E,F,G,H,O> onException(Class<T> exceptionType, ExceptionHandler<T, E,F,G,H> handler)
    {
        getHandlerMap().put(exceptionType, (ExceptionHandler) handler);
        return this;
    }

    /** Apply the retry logic.
     *
     * @return True, if we should continue retrying, false otherwise.
     */
    boolean retryLogic();
}
