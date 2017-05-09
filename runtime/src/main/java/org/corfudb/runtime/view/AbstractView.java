package org.corfudb.runtime.view;


import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.WrongEpochException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * All views inherit from AbstractView.
 * <p>
 * AbstractView requires a runtime, and provides a layoutHelper function.
 * <p>
 * The layoutHelper function is called whenever a view tries to access a layout.
 * If the layoutHelper catches an exception which is due to connection issues
 * or an incorrect epoch, it asks the runtime to invalidate that layout
 * by reporting it to a layout server, and retries the function.
 * <p>
 * Created by mwei on 12/10/15.
 */
@Slf4j
public abstract class AbstractView {

    CorfuRuntime runtime;

    public AbstractView(CorfuRuntime runtime) {
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
                return runtime.layoutFuture.get();
            } catch (Exception ex) {
                log.warn("Error executing remote call, invalidating view and retrying in {}s", runtime.retryRate, ex);
                runtime.invalidateLayout();
                try {
                    Thread.sleep(runtime.retryRate * 1000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Helper function for view to retrieve layouts.
     * This function will retry the given function indefinitely, invalidating the view if there was a exception
     * contacting the endpoint.
     *
     * @param function The function to execute.
     * @param <T>      The return type of the function.
     * @param <A>      Any exception the function may throw.
     * @param <B>      Any exception the function may throw.
     * @param <C>      Any exception the function may throw.
     * @param <D>      Any exception the function may throw.
     * @return The return value of the function.
     */
    public <T, A extends RuntimeException, B extends RuntimeException, C extends RuntimeException, D extends RuntimeException>
    T layoutHelper(LayoutFunction<Layout, T, A, B, C, D> function)
            throws A, B, C, D {
        while (true) {
            try {
                return function.apply(runtime.layoutFuture.get());
            } catch (RuntimeException re) {
                if (re.getCause() instanceof TimeoutException) {
                    log.warn("Timeout executing remote call, invalidating view and retrying in {}s", runtime.retryRate);
                    runtime.invalidateLayout();
                    try {
                        Thread.sleep(runtime.retryRate * 1000);
                    } catch (InterruptedException ie) {
                    }
                } else if (re instanceof WrongEpochException){
                    WrongEpochException we = (WrongEpochException) re;
                    log.warn("Got a wrong epoch exception, updating epoch to {} and invalidate view",
                        we.getCorrectEpoch());
                    Long newEpoch = (we.getCorrectEpoch());
                    runtime.nodeRouters.values().forEach(x -> x.setEpoch(newEpoch));
                    runtime.invalidateLayout();
                } else {
                    throw re;
                }
            } catch (InterruptedException | ExecutionException ex) {
                log.warn("Error executing remote call, invalidating view and retrying in {}s", runtime.retryRate, ex);
                runtime.invalidateLayout();
                try {
                    Thread.sleep(runtime.retryRate * 1000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    @FunctionalInterface
    public interface LayoutFunction<v, R, A extends Throwable,
            B extends Throwable, C extends Throwable, D extends Throwable> {
        R apply(Layout l) throws A, B, C, D;
    }
}
