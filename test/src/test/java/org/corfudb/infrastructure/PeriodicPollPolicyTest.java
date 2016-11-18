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

        addServer(9000);
        addServer(9001);
        addServer(9002);

        layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addLayoutServer(9002)
                .addSequencer(9000)
                .addSequencer(9001)
                .addSequencer(9002)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addLogUnit(9001)
                .addLogUnit(9002)
                .addToSegment()
                .addToLayout()
                .build();

        // Bootstrapping only the layout servers.
        // Failure-correction/updateLayout will cause test to give a non-deterministic output.

        getLayoutServer(9000).handleMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(layout)),
                null, getServerRouter(9000));
        getLayoutServer(9001).handleMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(layout)),
                null, getServerRouter(9001));
        getLayoutServer(9002).handleMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(layout)),
                null, getServerRouter(9002));

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

        for (int i = 0; i < 5; i++) {
            failureDetectorPolicy.executePolicy(layout, corfuRuntime);
            Thread.sleep(100);
        }

        // A little more than responseTimeout for periodicPolling
        Thread.sleep(1100);
        Map<String, Boolean> result = failureDetectorPolicy.getServerStatus();
        assertThat(result.isEmpty()).isEqualTo(true);

    }

    /**
     * Polls 3 failed servers.
     * Returns failed status for the 3 servers.
     * We then restart server 9000, run polls again.
     * Assert only 2 failures. 9001 & 9002
     *
     * @throws InterruptedException
     */
    @Test
    public void failedPolling() throws InterruptedException {

        addServerRule(9000, new TestRule().always().drop());
        addServerRule(9001, new TestRule().always().drop());
        addServerRule(9002, new TestRule().always().drop());

        for (int i = 0; i < 5; i++) {
            failureDetectorPolicy.executePolicy(layout, corfuRuntime);
            Thread.sleep(100);
        }

        Map<String, Boolean> expectedResult = new HashMap<>();
        expectedResult.put(getEndpoint(9000), false);
        expectedResult.put(getEndpoint(9001), false);
        expectedResult.put(getEndpoint(9002), false);

        Map<String, Boolean> actualResult = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            failureDetectorPolicy.getServerStatus().forEach(actualResult::putIfAbsent);
            Thread.sleep(200);
            if (actualResult.equals(expectedResult)) break;
        }

        assertThat(actualResult).isEqualTo(expectedResult);

        /*
         * Restarting the server 9000. Pings should work normally now.
         * This is also to demonstrate that we no longer receive the failed
         * nodes' status in the result map for 9000.
         */

        clearServerRules(9000);

        for (int i = 0; i < 5; i++) {
            failureDetectorPolicy.executePolicy(layout, corfuRuntime);
            Thread.sleep(100);
        }

        expectedResult.remove(getEndpoint(9000));

        actualResult = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            failureDetectorPolicy.getServerStatus().forEach(actualResult::putIfAbsent);
            Thread.sleep(200);
            if (actualResult.equals(expectedResult)) break;
        }

        // Has only 9001 & 9002
        assertThat(actualResult).isEqualTo(expectedResult);

    }
}
