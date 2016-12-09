package org.corfudb.runtime.view;

import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.clients.TestRule;
import org.junit.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify the Management Server functionality.
 * <p>
 * Created by zlokhandwala on 11/9/16.
 */
public class ManagementViewTest extends AbstractViewTest {

    /**
     * Boolean flag turned to true when the MANAGEMENT_FAILURE_DETECTED message
     * is sent by the Management client to its server.
     */
    private static final Semaphore failureDetected = new Semaphore(1, true);

    /**
     * Scenario with 2 nodes: 9000 and 9001.
     * We fail 9000 and then listen to intercept the message
     * sent by 9001's client to the server to handle the failure.
     *
     * @throws Exception
     */
    @Test
    public void invokeFailureHandler()
            throws Exception {
        addServer(9000);
        addServer(9001);
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addSequencer(9000)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addLogUnit(9001)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);

        // Reduce test execution time from 15+ seconds to about 8 seconds:
        // Set aggressive timeouts for surviving MS that polls the dead MS.
        ManagementServer ms = getManagementServer(9001);
        ms.getCorfuRuntime().getRouter("test:9000").setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        ms.getCorfuRuntime().getRouter("test:9000").setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        ms.getCorfuRuntime().getRouter("test:9000").setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());

        failureDetected.acquire();

        // Adding a rule on 9000 to drop all packets
        addServerRule(9000, new TestRule().always().drop());
        getManagementServer(9000).shutdown();

        // Adding a rule on 9001 to toggle the flag when it sends the
        // MANAGEMENT_FAILURE_DETECTED message.
        addClientRule(getManagementServer(9001).getCorfuRuntime(), new TestRule().matches(corfuMsg -> {
            if (corfuMsg.getMsgType().equals(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED)) {
                failureDetected.release();
            }
            return true;
        }));

        assertThat(failureDetected.tryAcquire(PARAMETERS.TIMEOUT_LONG.toNanos(),
                TimeUnit.NANOSECONDS)).isEqualTo(true);
    }
}
