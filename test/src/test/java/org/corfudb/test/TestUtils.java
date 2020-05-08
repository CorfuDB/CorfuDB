package org.corfudb.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.AbstractCorfuTest.PARAMETERS;

import java.util.function.Predicate;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.Sleep;

/**
 * Created by zlokhandwala on 2019-06-06.
 */
public class TestUtils {

    /**
     * Sets aggressive timeouts for all the router endpoints on all the runtimes.
     * <p>
     *
     * @param layout        Layout to get all server endpoints.
     * @param corfuRuntimes All runtimes whose routers' timeouts are to be set.
     */
    public static void setAggressiveTimeouts(Layout layout, CorfuRuntime... corfuRuntimes) {
        layout.getAllServers().forEach(routerEndpoint -> {
            for (CorfuRuntime runtime : corfuRuntimes) {
                runtime.getRouter(routerEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                runtime.getRouter(routerEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                runtime.getRouter(routerEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            }
        });
    }

    /**
     * Clears aggressive timeouts for all the router endpoints on all the runtimes.
     * <p>
     *
     * @param layout        Layout to get all server endpoints.
     * @param corfuRuntimes All runtimes whose routers' timeouts are to be set.
     */
    public static void clearAggressiveTimeouts(Layout layout, CorfuRuntime... corfuRuntimes) {
        layout.getAllServers().forEach(routerEndpoint -> {
            for (CorfuRuntime runtime : corfuRuntimes) {
                runtime.getRouter(routerEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_NORMAL.toMillis());
                runtime.getRouter(routerEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_NORMAL.toMillis());
                runtime.getRouter(routerEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_NORMAL.toMillis());
            }
        });
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected verifier.
     *
     * @param verifier     Layout predicate to test the refreshed layout.
     * @param corfuRuntime corfu runtime.
     */
    public static void waitForLayoutChange(Predicate<Layout> verifier, CorfuRuntime corfuRuntime) {
        corfuRuntime.invalidateLayout();
        Layout refreshedLayout = corfuRuntime.getLayoutView().getLayout();

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (verifier.test(refreshedLayout)) {
                break;
            }
            corfuRuntime.invalidateLayout();
            refreshedLayout = corfuRuntime.getLayoutView().getLayout();
            Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_VERY_SHORT);
        }
        assertThat(verifier.test(refreshedLayout)).isTrue();
    }

}
