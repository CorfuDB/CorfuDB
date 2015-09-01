package org.corfudb.util.retry;

import java.util.Map;

/**
 * The IRetry interface is used to support implementing retry loops.
 * Special exceptions can be handled by registering a handler, which
 * returns false if the loop should be aborted.
 *
 * Created by mwei on 9/1/15.
 */
public interface IRetry {

    /** Run the retry. This function may never return. */
    default void run()
    {
        boolean retry = true;
        while (retry) {
            try {
                retry = !getRunFunction().retryFunction();
            } catch (Exception e) {
                if (getHandlerMap().containsKey(e.getClass()))
                {
                    retry = getHandlerMap().get(e.getClass()).HandleException(e);
                }
                else {
                    retry = handleException(e);
                }
            }
        }
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
    interface ExceptionHandler {
        boolean HandleException(Exception e);
    }

    /**
     * Get the exception handler map for the retry logic.
     *
     * @return                  The exception handler map for the retry logic.
     */
    Map<Class<? extends Exception>, ExceptionHandler> getHandlerMap();

    /**
     * Register an exception handler when an exception occurs.
     * Note that only one exception handler can be registered per exception type.
     * @param exceptionType     The type of exception to handle.
     * @param handler           The action to take when an exception occurs. The handler should return true
     *                          if the retry logic should continue, or false if the logic should abort.
     * @return                  An IRetry, to support a fluent API interface.
     */
    default IRetry onException(Class<? extends Exception> exceptionType, ExceptionHandler handler)
    {
        getHandlerMap().put(exceptionType, handler);
        return this;
    }
}
