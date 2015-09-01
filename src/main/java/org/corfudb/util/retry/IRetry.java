package org.corfudb.util.retry;

/**
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
                retry = handleException(e);
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

}
