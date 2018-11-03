package org.corfudb.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.NodeLocator;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the FailureDetector.
 * <p>
 * Created by zlokhandwala on 10/26/16.
 */
public class FailureDetectorTest extends AbstractViewTest {

    private Layout layout = null;
    private CorfuRuntime corfuRuntime = null;
    private FailureDetector failureDetector = null;

    @Before
    public void pollingEnvironmentSetup() {

        addServer(SERVERS.ENDPOINT_0);
        addServer(SERVERS.ENDPOINT_1);
        addServer(SERVERS.ENDPOINT_2);

        layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addLayoutServer(SERVERS.ENDPOINT_2)
                .addSequencer(SERVERS.ENDPOINT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_0)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(layout);

        corfuRuntime = getRuntime(layout).connect();

        layout.getAllServersNodes().forEach(serverEndpoint -> {
            corfuRuntime.getRouter(serverEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            corfuRuntime.getRouter(serverEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            corfuRuntime.getRouter(serverEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        });

        // shutdown all management agents to avoid run of the fast object loader and all attempts to modify the layout
        getManagementServer(SERVERS.ENDPOINT_0).getManagementAgent().shutdown();
        getManagementServer(SERVERS.ENDPOINT_1).getManagementAgent().shutdown();
        getManagementServer(SERVERS.ENDPOINT_2).getManagementAgent().shutdown();

        failureDetector = new FailureDetector();
        failureDetector.setInitPeriodDuration(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        failureDetector.setPeriodDelta(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        failureDetector.setMaxPeriodDuration(PARAMETERS.TIMEOUT_SHORT.toMillis());
    }

    /**
     * Polls 3 running servers. Poll returns no failures.
     */
    @Test
    public void pollNoFailures() {

        // A little more than responseTimeout for periodicPolling
        PollReport result = failureDetector.poll(layout, corfuRuntime);
        assertThat(result.getFailingNodes()).isEmpty();
        assertThat(result.getOutOfPhaseEpochNodes()).isEmpty();

    }

    /**
     * Polls 3 failed servers.
     * Returns failed status for the 3 servers.
     * We then restart server SERVERS.ENDPOINT_0, run polls again.
     * Assert only 2 failures. SERVERS.ENDPOINT_1 & SERVERS.ENDPOINT_2
     */
    @Test
    public void pollFailures() {
        addServerRule(SERVERS.ENDPOINT_0, new TestRule().always().drop());
        addServerRule(SERVERS.ENDPOINT_1, new TestRule().always().drop());
        addServerRule(SERVERS.ENDPOINT_2, new TestRule().always().drop());

        Set<NodeLocator> expectedResult = new HashSet<>();
        expectedResult.add(SERVERS.ENDPOINT_0);
        expectedResult.add(SERVERS.ENDPOINT_1);
        expectedResult.add(SERVERS.ENDPOINT_2);

        PollReport pollReport = failureDetector.poll(layout, corfuRuntime);
        ImmutableSet<NodeLocator> failingNodes = pollReport.getfailingNodesEndpoints();
        assertThat(failingNodes).isEqualTo(expectedResult);

        /*
         * Restarting the server SERVERS.ENDPOINT_0. Pings should work normally now.
         * This is also to demonstrate that we no longer receive the failed
         * nodes' status in the result map for SERVERS.ENDPOINT_0.
         */

        clearServerRules(SERVERS.ENDPOINT_0);
        // Has only SERVERS.ENDPOINT_1 & SERVERS.ENDPOINT_2
        expectedResult.remove(SERVERS.ENDPOINT_0);

        assertThat(failureDetector.poll(layout, corfuRuntime).getfailingNodesEndpoints())
                .isEqualTo(expectedResult);

    }
}
