package org.corfudb.universe.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_WAIT_TIME;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

@Slf4j
public class ScenarioUtils {

    public static void waitForNextEpoch(CorfuClient corfuClient, long nextEpoch) {
        waitForLayoutChange(layout -> {
            if (layout.getEpoch() > nextEpoch) {
                throw new IllegalStateException("Layout epoch is ahead of next epoch. Next epoch: " + nextEpoch +
                        ", layout epoch: " + layout.getEpoch());
            }
            return layout.getEpoch() == nextEpoch;
        }, corfuClient);
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected verifier.
     *
     * @param verifier    Layout predicate to test the refreshed layout.
     * @param corfuClient corfu client.
     */
    public static void waitForLayoutChange(Predicate<Layout> verifier, CorfuClient corfuClient) {

        corfuClient.invalidateLayout();
        Layout refreshedLayout = corfuClient.getLayout();

        for (int i = 0; i < TestFixtureConst.DEFAULT_WAIT_POLL_ITER; i++) {
            if (verifier.test(refreshedLayout)) {
                break;
            }
            corfuClient.invalidateLayout();
            refreshedLayout = corfuClient.getLayout();
            sleep();
        }

        assertThat(verifier.test(refreshedLayout)).isTrue();
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected unresponsive servers size
     *
     * @param verifier    IntPredicate to test the refreshed unresponsive servers size
     * @param corfuClient corfu client.
     */
    public static void waitForUnresponsiveServersChange(
            IntPredicate verifier, CorfuClient corfuClient) {
        corfuClient.invalidateLayout();
        Layout refreshedLayout = corfuClient.getLayout();

        for (int i = 0; i < TestFixtureConst.DEFAULT_WAIT_POLL_ITER; i++) {
            if (verifier.test(refreshedLayout.getUnresponsiveServers().size())) {
                break;
            }
            corfuClient.invalidateLayout();
            refreshedLayout = corfuClient.getLayout();
            sleep();
        }

        assertThat(verifier.test(refreshedLayout.getUnresponsiveServers().size())).isTrue();
    }

    private static void sleep() {
        try {
            TimeUnit.SECONDS.sleep(DEFAULT_WAIT_TIME.getSeconds());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UniverseException("Interrupted", e);
        }
    }

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected all layout servers size
     *
     * @param verifier    IntPredicate to test the refreshed layout servers size
     * @param corfuClient corfu client.
     */
    public static void waitForLayoutServersChange(IntPredicate verifier, CorfuClient corfuClient) {
        corfuClient.invalidateLayout();
        Layout refreshedLayout = corfuClient.getLayout();

        for (int i = 0; i < TestFixtureConst.DEFAULT_WAIT_POLL_ITER; i++) {
            if (verifier.test(refreshedLayout.getAllServers().size())) {
                break;
            }
            corfuClient.invalidateLayout();
            refreshedLayout = corfuClient.getLayout();
            sleep();
        }

        assertThat(verifier.test(refreshedLayout.getAllServers().size())).isTrue();
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

    static void waitForClusterUp(CorfuTable table, String value) {
        for (int i = 0; i < 3; i++) {
            try {
                table.get(value);
                return;
            } catch (UnreachableClusterException e) {
                log.info("Successfully waited failure detector to detect cluster up");
            }

            waitUninterruptibly(Duration.ofSeconds(10));
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
