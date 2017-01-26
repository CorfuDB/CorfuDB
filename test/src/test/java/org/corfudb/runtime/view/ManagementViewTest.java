package org.corfudb.runtime.view;

import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.TestRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
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
    private static final Semaphore failureDetected = new Semaphore(1,
            true);

    /**
     * Scenario with 2 nodes: SERVERS.PORT_0 and SERVERS.PORT_1.
     * We fail SERVERS.PORT_0 and then listen to intercept the message
     * sent by SERVERS.PORT_1's client to the server to handle the failure.
     *
     * @throws Exception
     */
    @Test
    public void invokeFailureHandler()
            throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);

        CorfuRuntime corfuRuntime = new CorfuRuntime();
        l.getLayoutServers().forEach(corfuRuntime::addLayoutServer);
        corfuRuntime.connect();
        corfuRuntime.getRouter(SERVERS.ENDPOINT_1).getClient(ManagementClient.class).initiateFailureHandler().get();


        // Reduce test execution time from 15+ seconds to about 8 seconds:
        // Set aggressive timeouts for surviving MS that polls the dead MS.
        ManagementServer ms = getManagementServer(SERVERS.PORT_1);
        ms.getCorfuRuntime().getRouter(SERVERS.ENDPOINT_0).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        ms.getCorfuRuntime().getRouter(SERVERS.ENDPOINT_0).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        ms.getCorfuRuntime().getRouter(SERVERS.ENDPOINT_0).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());

        failureDetected.acquire();

        // Adding a rule on SERVERS.PORT_0 to drop all packets
        addServerRule(SERVERS.PORT_0, new TestRule().always().drop());
        getManagementServer(SERVERS.PORT_0).shutdown();

        // Adding a rule on SERVERS.PORT_1 to toggle the flag when it sends the
        // MANAGEMENT_FAILURE_DETECTED message.
        addClientRule(getManagementServer(SERVERS.PORT_1).getCorfuRuntime(),
                new TestRule().matches(corfuMsg -> {
            if (corfuMsg.getMsgType().equals(CorfuMsgType
                    .MANAGEMENT_FAILURE_DETECTED)) {
                failureDetected.release();
            }
            return true;
        }));

        assertThat(failureDetected.tryAcquire(PARAMETERS.TIMEOUT_NORMAL
                        .toNanos(),
                TimeUnit.NANOSECONDS)).isEqualTo(true);
    }

    /**
     * Scenario with 3 nodes: SERVERS.PORT_0, SERVERS.PORT_1 and SERVERS.PORT_2.
     * We fail SERVERS.PORT_1 and then wait for one of the other two servers to
     * handle this failure, propose a new layout and we assert on the epoch change.
     *
     * @throws Exception
     */
    @Test
    public void handleSingleNodeFailure()
            throws Exception{
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);

        CorfuRuntime corfuRuntime = new CorfuRuntime();
        l.getLayoutServers().forEach(corfuRuntime::addLayoutServer);
        corfuRuntime.connect();
        // Initiating all failure handlers.
        for (String server: l.getAllServers()) {
            corfuRuntime.getRouter(server).getClient(ManagementClient.class).initiateFailureHandler().get();
        }

        // Setting aggressive timeouts
        List<Integer> serverPorts = new ArrayList<> ();
        serverPorts.add(SERVERS.PORT_0);
        serverPorts.add(SERVERS.PORT_1);
        serverPorts.add(SERVERS.PORT_2);
        List<String> routerEndpoints = new ArrayList<> ();
        routerEndpoints.add(SERVERS.ENDPOINT_0);
        routerEndpoints.add(SERVERS.ENDPOINT_1);
        routerEndpoints.add(SERVERS.ENDPOINT_2);
        serverPorts.forEach(serverPort -> {
            routerEndpoints.forEach(routerEndpoint -> {
                getManagementServer(serverPort).getCorfuRuntime().getRouter(routerEndpoint).setTimeoutConnect(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                getManagementServer(serverPort).getCorfuRuntime().getRouter(routerEndpoint).setTimeoutResponse(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
                getManagementServer(serverPort).getCorfuRuntime().getRouter(routerEndpoint).setTimeoutRetry(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
            });
        });

        // Adding a rule on SERVERS.PORT_1 to drop all packets
        addServerRule(SERVERS.PORT_1, new TestRule().always().drop());
        getManagementServer(SERVERS.PORT_1).shutdown();

        for (int i=0; i<PARAMETERS.NUM_ITERATIONS_LOW; i++){
            corfuRuntime.invalidateLayout();
            if (corfuRuntime.getLayoutView().getLayout().getEpoch() == 2L) {break;}
            Thread.sleep(PARAMETERS.TIMEOUT_VERY_SHORT.toMillis());
        }
        Layout l2 = corfuRuntime.getLayoutView().getLayout();
        assertThat(l2.getEpoch()).isEqualTo(2L);
        assertThat(l2.getLayoutServers().size()).isEqualTo(2);
        assertThat(l2.getLayoutServers().contains(SERVERS.ENDPOINT_1)).isFalse();
    }
}
