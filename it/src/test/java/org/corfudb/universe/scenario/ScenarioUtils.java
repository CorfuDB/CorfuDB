package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_WAIT_TIME;

@Slf4j
public class ScenarioUtils {

    /**
     * Wait for the Supplier (some condition) to return true.
     * @param supplier
     */
    private static void waitFor(Supplier<Boolean> supplier) {
        while(!supplier.get()) {
            Sleep.sleepUninterruptibly(Duration.ofSeconds(DEFAULT_WAIT_TIME));
        }
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected verifier.
     *
     * @param verifier    Layout predicate to test the refreshed layout.
     * @param corfuClient corfu client.
     */
    public static void waitForLayoutChange(Predicate<Layout> verifier, CorfuClient corfuClient) {
        waitFor(() -> {
            corfuClient.invalidateLayout();
            Layout refreshedLayout = corfuClient.getLayout();
            return verifier.test(refreshedLayout);
        });
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected unresponsive servers size
     *
     * @param verifier    IntPredicate to test the refreshed unresponsive servers size
     * @param corfuClient corfu client.
     */
    public static void waitForUnresponsiveServersChange(IntPredicate verifier, CorfuClient corfuClient) {
        waitFor(() -> {
            corfuClient.invalidateLayout();
            Layout refreshedLayout = corfuClient.getLayout();
            return verifier.test(refreshedLayout.getUnresponsiveServers().size());
        });
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected all layout servers size
     *
     * @param verifier    IntPredicate to test the refreshed layout servers size
     * @param corfuClient corfu client.
     */
    public static void waitForLayoutServersChange(IntPredicate verifier, CorfuClient corfuClient) {
        waitFor(() -> {
            corfuClient.invalidateLayout();
            Layout refreshedLayout = corfuClient.getLayout();
            return verifier.test(refreshedLayout.getAllServers().size());
        });
    }

    /**
     * Wait for failure detector to detect the cluster is down by generating a write request.
     * The runtime's systemDownHandler will be invoked after a limited time of retries
     * This method should only be called only after the cluster is unavailable
     *
     * @param table CorfuTable to generate write request
     */
    @SuppressWarnings("unchecked")
    static void waitForClusterDown(CorfuTable table) {
        try {
            table.put(new Object(), new Object());
            fail("Cluster should already be down");
        } catch (UnreachableClusterException e) {
            log.info("Successfully waited failure detector to detect cluster down");
        }
    }

    /**
     * Wait for a specific amount of time. This should only be used when there is nothing
     * else we can wait on, e.g. no layout change, no cluster status change.
     *
     * @param duration duration to wait
     */
    static void waitUninterruptibly(Duration duration) {
        Sleep.sleepUninterruptibly(duration);
    }
}
