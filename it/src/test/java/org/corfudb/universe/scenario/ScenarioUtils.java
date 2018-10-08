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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_WAIT_TIME;

@Slf4j
public class ScenarioUtils {

    /**
     * Refreshes the layout and waits for a limited time for the refreshed layout to
     * satisfy the expected unresponsive servers size
     *
     * @param verifier    IntPredicate to test the refreshed unresponsive servers size
     * @param corfuClient corfu client.
     */
    public static void waitForUnresponsiveServersChange(IntPredicate verifier, CorfuClient corfuClient) {
        corfuClient.invalidateLayout();
        Layout refreshedLayout = corfuClient.getLayout();

        for (int i = 0; i < TestFixtureConst.DEFAULT_WAIT_POLL_ITER; i++) {
            if (verifier.test(refreshedLayout.getUnresponsiveServers().size())) {
                break;
            }
            corfuClient.invalidateLayout();
            refreshedLayout = corfuClient.getLayout();
            Sleep.sleepUninterruptibly(Duration.ofSeconds(DEFAULT_WAIT_TIME));
        }

        assertThat(verifier.test(refreshedLayout.getUnresponsiveServers().size())).isTrue();
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
            Sleep.sleepUninterruptibly(Duration.ofSeconds(DEFAULT_WAIT_TIME));
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
}
