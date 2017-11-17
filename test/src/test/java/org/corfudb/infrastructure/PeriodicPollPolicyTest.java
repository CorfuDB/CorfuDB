package org.corfudb.infrastructure;

import org.corfudb.infrastructure.management.IFailureDetectorPolicy;
import org.corfudb.infrastructure.management.PeriodicPollPolicy;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the Periodic Polling Policy.
 * <p>
 * Created by zlokhandwala on 10/26/16.
 */
public class PeriodicPollPolicyTest extends AbstractViewTest {

    private IFailureDetectorPolicy failureDetectorPolicy = null;
    private Layout layout = null;
    private CorfuRuntime corfuRuntime = null;

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
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(layout);
        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();

        corfuRuntime = new CorfuRuntime();
        layout.getLayoutServers().forEach(corfuRuntime::addLayoutServer);
        corfuRuntime.connect();

        layout.getAllServers().forEach(serverEndpoint -> {
            corfuRuntime.getRouter(serverEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            corfuRuntime.getRouter(serverEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            corfuRuntime.getRouter(serverEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        });

        failureDetectorPolicy = new PeriodicPollPolicy();
    }

    /**
     * Polls 3 running servers. Poll returns no failures.
     *
     * @throws InterruptedException Sleep interrupted
     */
    @Test
    public void successfulPolling() throws InterruptedException {

        for (int i = 0; i < PARAMETERS.CONCURRENCY_SOME; i++) {
            failureDetectorPolicy.executePolicy(layout, corfuRuntime);
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }

        // A little more than responseTimeout for periodicPolling
        Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
        PollReport result = failureDetectorPolicy.getServerStatus();
        assertThat(result.getIsFailurePresent()).isFalse();

    }

    /**
     * Polls 3 failed servers.
     * Returns failed status for the 3 servers.
     * We then restart server SERVERS.PORT_0, run polls again.
     * Assert only 2 failures. SERVERS.PORT_1 & SERVERS.PORT_2
     *
     * @throws InterruptedException
     */
    @Test
    public void failedPolling() throws InterruptedException {

        addServerRule(SERVERS.PORT_0, new TestRule().always().drop());
        addServerRule(SERVERS.PORT_1, new TestRule().always().drop());
        addServerRule(SERVERS.PORT_2, new TestRule().always().drop());

        Set<String> expectedResult = new HashSet<>();
        expectedResult.add(getEndpoint(SERVERS.PORT_0));
        expectedResult.add(getEndpoint(SERVERS.PORT_1));
        expectedResult.add(getEndpoint(SERVERS.PORT_2));

        pollAndMatchExpectedResult(expectedResult);

        /*
         * Restarting the server SERVERS.PORT_0. Pings should work normally now.
         * This is also to demonstrate that we no longer receive the failed
         * nodes' status in the result map for SERVERS.PORT_0.
         */

        clearServerRules(SERVERS.PORT_0);
        // Has only SERVERS.PORT_1 & SERVERS.PORT_2
        expectedResult.remove(getEndpoint(SERVERS.PORT_0));

        pollAndMatchExpectedResult(expectedResult);

    }

    private void pollAndMatchExpectedResult(Set<String> expectedResult)
            throws InterruptedException {

        final int pollsToDeclareFailure = 10;
        for (int i = 0; i < pollsToDeclareFailure; i++) {
            failureDetectorPolicy.executePolicy(layout, corfuRuntime);
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }

        Set<String> actualResult = new HashSet<>();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LARGE; i++) {
            Set<String> tempResult = failureDetectorPolicy.getServerStatus().getFailingNodes();
            if (tempResult != null) {
                tempResult.forEach(actualResult::add);
            }
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            if (actualResult.equals(expectedResult)) break;
        }
        assertThat(actualResult).isEqualTo(expectedResult);
    }
}
