package org.corfudb.util.retry;

import lombok.SneakyThrows;

import java.util.Map;

/**
 * The IRetry interface is used to support implementing retry loops.
 * Special exceptions can be handled by registering a handler, which
 * returns false if the loop should be aborted.
 *
 * Created by mwei on 9/1/15.
 */
public interface IRetry<E extends Exception, F extends Exception, G extends Exception, H extends Exception> {

    /** Run the retry. This function may never return. */
    @SuppressWarnings("unchecked")
    default void run()
    throws E, F, G, H
    {
        boolean retry = true;
        while (retry) {
            try {
                retry = !getRunFunction().retryFunction();
            }
            catch (Exception ex) {
                if (getHandlerMap().containsKey(ex.getClass()))
                {
                    retry = ((ExceptionHandler<Exception, E, F, G ,H>)getHandlerMap().get(ex.getClass())).HandleException(ex) && handleException(ex);
                }
                else {
                    retry = handleException(ex);
                }
            }
        }
    }

    /* The following are builders which are basically a hack to support varidic exceptions... */

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static IRetry<RuntimeException, RuntimeException, RuntimeException, RuntimeException> build(Class<? extends IRetry> retryType, IRetryable runFunction)
    {
        return (IRetry<RuntimeException, RuntimeException, RuntimeException, RuntimeException>) retryType.getConstructor(IRetryable.class).newInstance(runFunction);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception> IRetry<T, RuntimeException, RuntimeException, RuntimeException>
    build(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, IRetryable runFunction)
    {
        return (IRetry<T, RuntimeException, RuntimeException, RuntimeException>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                                                                                    .passException(firstExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception> IRetry<T, U, RuntimeException, RuntimeException>
    build(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType, IRetryable runFunction)
    {
        return (IRetry<T,U, RuntimeException, RuntimeException>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, V extends Exception> IRetry<T, U, V, RuntimeException>
    build(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType, Class<? extends V> thirdExceptionType, IRetryable runFunction)
    {
        return (IRetry<T,U, V, RuntimeException>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType)
                .passException(thirdExceptionType);
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    static <T extends Exception, U extends Exception, V extends Exception, W extends Exception>
    IRetry<T, U, V, W> build4(Class<? extends IRetry> retryType, Class<? extends T> firstExceptionType, Class<? extends U> secondExceptionType,
                              Class<? extends V> thirdExceptionType,  Class<? extends W> fourthExceptionType,  IRetryable runFunction)
    {
        return (IRetry<T,U, V, W>) retryType.getConstructor(IRetryable.class).newInstance(runFunction)
                .passException(firstExceptionType)
                .passException(secondExceptionType)
                .passException(thirdExceptionType)
                .passException(fourthExceptionType);
    }

    /** Get the function that needs to be retried. */
    IRetryable getRunFunction();

    /** Handle an exception which has occurred and that has not been registered.
     * @param e     The exception that has occurred.
     * @return      True, to continue retrying, or False, to stop running the function.
     */
    boolean handleException(Exception e);

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
    default <T extends Exception> IRetry<E,F,G,H> passException(Class<T> exceptionType)
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
    default <T extends Exception> IRetry<E,F,G,H> onException(Class<T> exceptionType, ExceptionHandler<T, E,F,G,H> handler)
    {
        getHandlerMap().put(exceptionType, (ExceptionHandler) handler);
        return this;
    }
}
