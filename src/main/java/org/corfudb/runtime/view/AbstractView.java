package org.corfudb.runtime.view;


import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public abstract class AbstractView {

    CorfuRuntime runtime;

    public AbstractView(CorfuRuntime runtime)
    {
        this.runtime = runtime;
    }

    @FunctionalInterface
    public interface LayoutFunction<Layout, R> {
        R apply(Layout l) throws Exception;
    }

    /** Helper function for view to retrieve layouts.
     * This function will retry the given function indefinitely, invalidating the view if there was a exception
     * contacting the endpoint.
     * @param function  The function to execute.
     * @param <T>       The return type of the function.
     * @return          The return value of the function.
     */
    public <T> T layoutHelper (LayoutFunction<Layout,T> function)
    {
        while(true) {
            try {
                return function.apply(runtime.layout.get());
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
}
