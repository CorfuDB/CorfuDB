package org.corfudb.runtime.view;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.exceptions.WrongEpochException;
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
     * Maintains reference of the RuntimeLayout which gets invalidated if the AbstractView
     * encounters a newer layout. (with a greater epoch)
     */
    private AtomicReference<RuntimeLayout> runtimeLayout = new AtomicReference<>();

    /**
     * Fetches the layout uninterruptibly and rethrows any systemDownHandlerExceptions or Errors
     * encountered.
     *
     * @return Fetched layout
     */
    private Layout getLayoutUninterruptibly() {
        try {
            return runtime.layout.get();
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError("Interrupted in layoutHelper", ie);
        } catch (ExecutionException ex) {
            // Invalidate layout since the layout completable future now contains an exception.
            runtime.invalidateLayout();
            // If an error or an unchecked exception is thrown by the layout.get() completable
            // future, the exception will materialize as an ExecutionException. In that case,
            // we need to propagate this Error or unchecked exception.
            if (ex.getCause() instanceof Error) {
                log.error("getLayoutUninterruptibly: Encountered error. Aborting layoutHelper", ex);
                throw (Error) ex.getCause();
            }

            if (ex.getCause() instanceof RuntimeException) {
                log.error("getLayoutUninterruptibly: Encountered unchecked exception. "
                        + "Aborting layoutHelper", ex);
                throw (RuntimeException) ex.getCause();
            }

            log.error("getLayoutUninterruptibly: Encountered exception while fetching layout");
            throw new RuntimeException(ex);
        }
    }

    /**
     * Helper function for view to retrieve layouts.
     * This function will retry the given function indefinitely,
     * invalidating the view if there was a exception contacting the endpoint.
     *
     * There is a flag to set if we want the caller to handle Runtime Exceptions. For some
     * special cases (like writes), we need to do a bit more work upon a Runtime Exception than just retry.
     *
     * @param function             The function to execute.
     * @param <T>                  The return type of the function.
     * @param <A>                  Any exception the function may throw.
     * @param <B>                  Any exception the function may throw.
     * @param <C>                  Any exception the function may throw.
     * @param <D>                  Any exception the function may throw.
     * @param rethrowAllExceptions if all exceptions are rethrown to caller.
     * @return The return value of the function.
     */
    public <T, A extends RuntimeException, B extends RuntimeException, C extends RuntimeException,
            D extends RuntimeException> T layoutHelper(LayoutFunction<Layout, T, A, B, C, D>
                                                               function,
                                                       boolean rethrowAllExceptions)
            throws A, B, C, D {
        runtime.getParameters().getBeforeRpcHandler().run();
        final Duration retryRate = runtime.getParameters().getConnectionRetryRate();
        int systemDownTriggerCounter = 0;
        while (true) {

            final Layout layout = getLayoutUninterruptibly();

            try {
                return function.apply(runtimeLayout.updateAndGet(rLayout -> {
                    if (rLayout == null || rLayout.getLayout().getEpoch() != layout.getEpoch()) {
                        return new RuntimeLayout(layout, runtime);
                    }
                    return rLayout;
                }));
            } catch (RuntimeException re) {
                if (re.getCause() instanceof TimeoutException) {
                    log.warn("Timeout executing remote call, invalidating view and retrying "
                            + "in {}s", retryRate);
                } else if (re instanceof ServerNotReadyException) {
                    log.warn("Server still not ready. Waiting for server to start "
                            + "accepting requests.");
                } else if (re instanceof WrongEpochException) {
                    WrongEpochException we = (WrongEpochException) re;
                    log.warn("Got a wrong epoch exception, updating epoch to {} and "
                            + "invalidate view", we.getCorrectEpoch());
                } else if (re instanceof NetworkException) {
                    log.warn("layoutHelper: System seems unavailable", re);
                } else {
                    throw re;
                }
                if (rethrowAllExceptions) {
                    throw new RuntimeException(re);
                }
            }

            log.info("layoutHelper: Retried {} times, SystemDownHandlerTriggerLimit = {}",
                    systemDownTriggerCounter,
                    runtime.getParameters().getSystemDownHandlerTriggerLimit());

            // Invoking the systemDownHandler if the client cannot connect to the server.
            if (++systemDownTriggerCounter
                    >= runtime.getParameters().getSystemDownHandlerTriggerLimit()) {
                log.info("layoutHelper: Invoking the systemDownHandler.");
                runtime.getParameters().getSystemDownHandler().run();
            }

            runtime.invalidateLayout();
            Sleep.sleepUninterruptibly(retryRate);
        }
    }

    @FunctionalInterface
    public interface LayoutFunction<V, R, A extends Throwable,
            B extends Throwable, C extends Throwable, D extends Throwable> {
        R apply(RuntimeLayout l) throws A, B, C, D;
    }
}
