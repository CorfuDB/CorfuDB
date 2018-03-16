package org.corfudb.runtime.view;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.unrecoverable.SystemUnavailableError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.util.Sleep;

/**
 * All views inherit from AbstractView.
 *
 * <p>AbstractView requires a runtime, and provides a layoutHelper function.</p>
 *
 * <p>The layoutHelper function is called whenever a view tries to access a layout.
 * If the layoutHelper catches an exception which is due to connection issues
 * or an incorrect epoch, it asks the runtime to invalidate that layout
 * by reporting it to a layout server, and retries the function.</p>
 *
 * <p>Created by mwei on 12/10/15.</p>
 */
@Slf4j
public abstract class AbstractView {

    final CorfuRuntime runtime;

    public AbstractView(@Nonnull final CorfuRuntime runtime) {
        this.runtime = runtime;
    }

    /**
     * Get the current layout.
     *
     * @return The current layout.
     */
    public Layout getCurrentLayout() {
        while (true) {
            try {
                return runtime.layout.get();
            } catch (Exception ex) {
                log.warn("Error executing remote call, invalidating view and retrying in {}s",
                        runtime.getParameters().getConnectionRetryRate(), ex);
                runtime.invalidateLayout();
                Sleep.sleepUninterruptibly(runtime.getParameters().getConnectionRetryRate());
            }
        }
    }

    public <T, A extends RuntimeException, B extends RuntimeException, C extends RuntimeException,
            D extends RuntimeException> T layoutHelper(LayoutFunction<Layout, T, A, B, C, D>
                                                               function)
            throws A, B, C, D {
        return layoutHelper(function, false);
    }


    /**
     * Helper function for view to retrieve layouts.
     * This function will retry the given function indefinitely,
     * invalidating the view if there was a exception contacting the endpoint.
     *
     * There is a flag to set if we want the caller to handle Runtime Exceptions. For some
     * special cases (like writes), we need to do a bit more work upon a Runtime Exception than just retry.
     *
     * @param function The function to execute.
     * @param <T>      The return type of the function.
     * @param <A>      Any exception the function may throw.
     * @param <B>      Any exception the function may throw.
     * @param <C>      Any exception the function may throw.
     * @param <D>      Any exception the function may throw.
     * @param rethrowAllExceptions if all exceptions are rethrown to caller.
     * @return The return value of the function.
     */
    public <T, A extends RuntimeException, B extends RuntimeException, C extends RuntimeException,
            D extends RuntimeException> T layoutHelper(LayoutFunction<Layout, T, A, B, C, D>
                                                                          function,
                                                       boolean rethrowAllExceptions)
            throws A, B, C, D {
        runtime.beforeRpcHandler.run();
        final Duration retryRate = runtime.getParameters().getConnectionRetryRate();
        while (true) {
            try {
                return function.apply(runtime.layout.get());
            } catch (RuntimeException re) {
                if (re.getCause() instanceof TimeoutException) {
                    log.warn("Timeout executing remote call, invalidating view and retrying in {}s",
                            retryRate);
                    runtime.invalidateLayout();
                    Sleep.sleepUninterruptibly(retryRate);
                } else if (re instanceof ServerNotReadyException) {
                    log.warn("Server still not ready. Waiting for server to start "
                            + "accepting requests.");
                    Sleep.sleepUninterruptibly(retryRate);
                } else if (re instanceof WrongEpochException) {
                    WrongEpochException we = (WrongEpochException) re;
                    log.warn("Got a wrong epoch exception, updating epoch to {} and "
                            + "invalidate view", we.getCorrectEpoch());
                    runtime.invalidateLayout();
                } else if (re instanceof NetworkException) {
                    log.warn("layoutHelper: System seems unavailable", re);

                    runtime.systemDownHandler.run();
                    runtime.invalidateLayout();
                    Sleep.sleepUninterruptibly(retryRate);
                } else {
                    throw re;
                }
                if (rethrowAllExceptions) {
                    throw new RuntimeException(re);
                }

            } catch (InterruptedException ie) {
                throw new UnrecoverableCorfuInterruptedError("Interrupted in layoutHelper", ie);
            } catch (ExecutionException ex) {
                log.warn("Error executing remote call, invalidating view and retrying in {} ms",
                        retryRate, ex);

                // If SystemUnavailable exception is thrown by the layout.get() completable future,
                // the exception will materialize as an ExecutionException. In that case, we need to propagate
                // this exception.
                if (ex.getCause() instanceof SystemUnavailableError) {
                    throw (SystemUnavailableError) ex.getCause();
                }

                runtime.invalidateLayout();
                Sleep.sleepUninterruptibly(retryRate);
            }
        }
    }

    @FunctionalInterface
    public interface LayoutFunction<V, R, A extends Throwable,
            B extends Throwable, C extends Throwable, D extends Throwable> {
        R apply(Layout l) throws A, B, C, D;
    }
}
