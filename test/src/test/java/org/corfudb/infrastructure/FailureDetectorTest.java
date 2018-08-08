package org.corfudb.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
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

        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(layout);

        corfuRuntime = getRuntime(layout).connect();

        layout.getAllServers().forEach(serverEndpoint -> {
            corfuRuntime.getRouter(serverEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            corfuRuntime.getRouter(serverEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            corfuRuntime.getRouter(serverEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        });

        // shutdown all management agents to avoid run of the fast object loader and all attempts to modify the layout
        getManagementServer(SERVERS.PORT_0).getManagementAgent().shutdown();
        getManagementServer(SERVERS.PORT_1).getManagementAgent().shutdown();
        getManagementServer(SERVERS.PORT_2).getManagementAgent().shutdown();

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
     * We then restart server SERVERS.PORT_0, run polls again.
     * Assert only 2 failures. SERVERS.PORT_1 & SERVERS.PORT_2
     */
    @Test
    public void pollFailures() {
        addServerRule(SERVERS.PORT_0, new TestRule().always().drop());
        addServerRule(SERVERS.PORT_1, new TestRule().always().drop());
        addServerRule(SERVERS.PORT_2, new TestRule().always().drop());

        Set<String> expectedResult = new HashSet<>();
        expectedResult.add(getEndpoint(SERVERS.PORT_0));
        expectedResult.add(getEndpoint(SERVERS.PORT_1));
        expectedResult.add(getEndpoint(SERVERS.PORT_2));

        assertThat(failureDetector.poll(layout, corfuRuntime).getFailingNodes())
                .isEqualTo(expectedResult);

        /*
         * Restarting the server SERVERS.PORT_0. Pings should work normally now.
         * This is also to demonstrate that we no longer receive the failed
         * nodes' status in the result map for SERVERS.PORT_0.
         */

        clearServerRules(SERVERS.PORT_0);
        // Has only SERVERS.PORT_1 & SERVERS.PORT_2
        expectedResult.remove(getEndpoint(SERVERS.PORT_0));

        assertThat(failureDetector.poll(layout, corfuRuntime).getFailingNodes())
                .isEqualTo(expectedResult);

    }
}
