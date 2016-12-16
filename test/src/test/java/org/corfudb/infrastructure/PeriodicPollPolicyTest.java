package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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

        // Bootstrapping only the layout servers.
        // Failure-correction/updateLayout will cause test to give a non-deterministic output.

        getLayoutServer(SERVERS.PORT_0).handleMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(layout)),
                null, getServerRouter(SERVERS.PORT_0));
        getLayoutServer(SERVERS.PORT_1).handleMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(layout)),
                null, getServerRouter(SERVERS.PORT_1));
        getLayoutServer(SERVERS.PORT_2).handleMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(layout)),
                null, getServerRouter(SERVERS.PORT_2));

        corfuRuntime = new CorfuRuntime();
        layout.getLayoutServers().forEach(corfuRuntime::addLayoutServer);
        corfuRuntime.connect();

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
        Map<String, Boolean> result = failureDetectorPolicy.getServerStatus();
        assertThat(result.isEmpty()).isEqualTo(true);

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

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            failureDetectorPolicy.executePolicy(layout, corfuRuntime);
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }

        Map<String, Boolean> expectedResult = new HashMap<>();
        expectedResult.put(getEndpoint(SERVERS.PORT_0), false);
        expectedResult.put(getEndpoint(SERVERS.PORT_1), false);
        expectedResult.put(getEndpoint(SERVERS.PORT_2), false);

        Map<String, Boolean> actualResult = new HashMap<>();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LARGE; i++) {
            failureDetectorPolicy.getServerStatus().forEach(actualResult::putIfAbsent);
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            if (actualResult.equals(expectedResult)) break;
        }

        assertThat(actualResult).isEqualTo(expectedResult);

        /*
         * Restarting the server SERVERS.PORT_0. Pings should work normally now.
         * This is also to demonstrate that we no longer receive the failed
         * nodes' status in the result map for SERVERS.PORT_0.
         */

        clearServerRules(SERVERS.PORT_0);

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            failureDetectorPolicy.executePolicy(layout, corfuRuntime);
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }

        expectedResult.remove(getEndpoint(SERVERS.PORT_0));

        actualResult = new HashMap<>();
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LARGE; i++) {
            failureDetectorPolicy.getServerStatus().forEach(actualResult::putIfAbsent);
            Thread.sleep(PARAMETERS.TIMEOUT_SHORT.toMillis());
            if (actualResult.equals(expectedResult)) break;
        }

        // Has only SERVERS.PORT_1 & SERVERS.PORT_2
        assertThat(actualResult).isEqualTo(expectedResult);

    }
}
