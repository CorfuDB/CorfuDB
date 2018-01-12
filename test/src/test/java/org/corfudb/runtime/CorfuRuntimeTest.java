package org.corfudb.runtime;

import org.corfudb.infrastructure.TestLayoutBuilder;

import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.clients.TestClientRouter;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.exceptions.unrecoverable.SystemUnavailableError;

import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


/**
 * Created by maithem on 6/21/16.
 */
public class CorfuRuntimeTest extends AbstractViewTest {
    static final int TIME_TO_WAIT_FOR_LAYOUT_IN_SEC = 5;
    static final long TIMEOUT_CORFU_RUNTIME_IN_MS = 500;



    /**
     * Resets the router function to the default function for AbstractViewTest.
     */
    @Before
    public void setDefaultRuntimeGetRouterFunction() {
        CorfuRuntime.overrideGetRouterFunction =
                (runtime, endpoint) -> super.getRouterFunction(runtime, endpoint);
    }

    @Test
    public void checkValidLayout() throws Exception {

        CorfuRuntime rt = getDefaultRuntime().connect();

        // Check that access to the CorfuRuntime layout is always valid. Specifically, access to the layout
        // while a new layout is being fetched/set concurrently.

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LARGE, (v) -> {
            rt.invalidateLayout();

        });

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LARGE, (v) -> {
            assertThat(rt.layout.get().getRuntime()).isEqualTo(rt);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_TWO, PARAMETERS.TIMEOUT_LONG);

    }

    @Test
    public void canInstantiateRuntimeWithoutTestRef() throws Exception {

        addSingleServer(SERVERS.PORT_0);

        CorfuRuntime rt = getNewRuntime(getDefaultNode());
        rt.connect();

    }

    /**
     * Generates and bootstraps a 3 node cluster.
     * Shuts down the management servers of the 3 nodes.
     *
     * @return The generated layout.
     * @throws Exception
     */
    private Layout get3NodeLayout() throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);
        Layout l = new TestLayoutBuilder()
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

        bootstrapAllServers(l);

        // Shutdown management server (they interfere)
        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();

        return l;
    }

    /**
     * Ensures that we will not accept a Layout that is obsolete.
     *
     * Test storyline:
     * 1. Seal the 3 servers
     * 2. Install a new Layout only on 2 of them
     * 3. Force the client to receive the Layout only from the staled Layout server.
     * 4. Ensure that we will never accept it.
     *
     * @throws Exception
     */
    @Test
    public void doesNotUpdateToLayoutWithSmallerEpoch() throws Exception {

        CorfuRuntime rt = getRuntime(get3NodeLayout()).connect();

        // Seal
        Layout currentLayout = new Layout(rt.getLayoutView().getCurrentLayout());
        currentLayout.setRuntime(rt);
        currentLayout.setEpoch(currentLayout.getEpoch() + 1);
        currentLayout.moveServersToEpoch();

        // Server2 is sealed but will not be able to commit the layout.
        addClientRule(rt, SERVERS.ENDPOINT_2,
                new TestRule().always().drop());


        rt.getLayoutView().updateLayout(currentLayout, 0);

        assertThat(getLayoutServer(SERVERS.PORT_2).getCurrentLayout().getEpoch() == 1);

        // Timeout for this server is 1ms, so it will fail fast
        rt.getRouter(SERVERS.ENDPOINT_2).setTimeoutResponse(1);

        // Invalidate and force this thread to wait for completion of the layout future.
        rt.invalidateLayout();
        rt.getLayoutView().getLayout();

        // Server 2 is back alive and its timeout back to normal
        clearClientRules(rt);
        rt.getRouter(SERVERS.ENDPOINT_2).setTimeoutResponse(PARAMETERS.TIMEOUT_NORMAL.toMillis());

        // Server 0 and Server 1 are not able to respond to layout request
        // Reduce their timeout to speed up the test.
        addClientRule(rt, SERVERS.ENDPOINT_0,
                new TestRule().always().drop());
        addClientRule(rt, SERVERS.ENDPOINT_1,
                new TestRule().always().drop());

        rt.getRouter(SERVERS.ENDPOINT_0).setTimeoutResponse(1);
        rt.getRouter(SERVERS.ENDPOINT_1).setTimeoutResponse(1);

        // Client gets layout from the staling LayoutServer but doesn't update it's layout.
        rt.invalidateLayout();

        // Ensure that we never (never is time_to_wait seconds here) get a new layout.
        CompletableFuture cf = CFUtils.within(rt.layout, Duration.ofSeconds(TIME_TO_WAIT_FOR_LAYOUT_IN_SEC));
        CompletableFuture.supplyAsync(() -> cf);

        assertThatThrownBy(() -> cf.get()).isInstanceOf(ExecutionException.class).hasRootCauseInstanceOf(TimeoutException.class);
    }


    @Test
    public void doesNotAllowReadsAfterSealAndBeforeNewLayout() throws Exception {
        CorfuRuntime runtime = getDefaultRuntime().setCacheDisabled(true).connect();

        Layout l = TestLayoutBuilder.single(0);
        bootstrapAllServers(l);

        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("test"));
        sv.append("testPayload".getBytes());

        l.setRuntime(runtime);
        l.setEpoch(l.getEpoch() + 1);
        l.moveServersToEpoch();

        // We need to be sure that the layout is invalidated before proceeding
        // This is what would trigger the wrong epoch exception in the consequent read.
        runtime.invalidateLayout();
        runtime.layout.get();

        LogUnitClient luc = runtime.getRouter(SERVERS.ENDPOINT_0).getClient(LogUnitClient.class);

        assertThatThrownBy(() -> luc.read(0).get())
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(WrongEpochException.class);
    }

    /**
     * Implement a SystemUnavailable systemDownHandler that stops the runtime after a certain timeout.
     *
     * The correct behaviour for this systemDownHandler is that, once the router is disconnected during the whole timeout,
     * a SystemUnavailableError is thrown and the runtime is shutdown.
     *
     * @throws Exception
     */
    @Test
    public void customNetworkExceptionHandler() throws Exception {
        class TimeoutHandler {
            CorfuRuntime rt;
            long maxTimeout;
            ThreadLocal<Long> localTimeStart = new ThreadLocal<>();

            TimeoutHandler(CorfuRuntime rt, long maxTimeout) {
                this.rt = rt;
                this.maxTimeout = maxTimeout;
            }

            void startTimeout() {
                localTimeStart.set(System.currentTimeMillis());
            }

            void checkIfTimeout() {
                if (System.currentTimeMillis() - localTimeStart.get() > maxTimeout) {
                    stopRuntimeAndThrowException();
                }
            }

            void stopRuntimeAndThrowException() {
                rt.stop();
                throw new SystemUnavailableError("Timeout " + maxTimeout + " elapsed");
            }
        }

        addSingleServer(SERVERS.PORT_0);

        CorfuRuntime runtime = getDefaultRuntime();
        TimeoutHandler th = new TimeoutHandler(runtime, TIMEOUT_CORFU_RUNTIME_IN_MS);

        runtime
                .registerBeforeRpcHandler(() -> th.startTimeout())
                .registerSystemDownHandler(() -> th.checkIfTimeout())
                .connect();

        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("test"));
        sv.append("testPayload".getBytes());

        simulateEndpointDisconnected(runtime);

        assertThatThrownBy(() -> sv.append("testPayload".getBytes())).
                isInstanceOf(SystemUnavailableError.class);

    }


     /**
     * Creates and bootstraps 3 nodes N0, N1 and N2.
     * The runtime connects to the 3 nodes and sets the clientRouter epochs to 1, 1 and 1.
     * Now the epoch is updated to 2.
     * We now simulate a NetworkException while fetching a router to the first node
     * in the list. The runtime is now invalidated, which forces to update the client router
     * epochs. This should now update the epochs to the following: 1, 2, 2.
     *
     * @throws Exception
     */
    @Test
    public void testFailedRouterInFetchLayout() throws Exception {

        final Map<String, TestClientRouter> routerMap = new ConcurrentHashMap<>();
        final AtomicReference<String> failedNode = new AtomicReference<>();

        // This getRouterFunction simulates the network exception thrown during the
        // NettyClientRouter creation.
        // Note: This does not simulate NetworkException while message transfer or connection.
        CorfuRuntime.overrideGetRouterFunction = (corfuRuntime, endpoint) -> {
            if (failedNode.get() != null && endpoint.equals(failedNode.get())) {
                throw new NetworkException("Test server not responding : ",
                    NodeLocator.builder()
                        .host(endpoint.split(":")[0])
                        .port(Integer.parseInt(endpoint.split(":")[1]))
                        .build());
            }
            if (!endpoint.startsWith("test:")) {
                throw new RuntimeException("Unsupported endpoint in test: " + endpoint);
            }
            return routerMap.computeIfAbsent(endpoint,
                    x -> {
                        TestClientRouter tcn =
                                new TestClientRouter(
                                        (TestServerRouter) getServerRouter(getPort(endpoint)));
                        tcn.addClient(new BaseClient())
                                .addClient(new SequencerClient())
                                .addClient(new LayoutClient())
                                .addClient(new LogUnitClient())
                                .addClient(new ManagementClient());
                        return tcn;
                    }
            );
        };

        Layout l = get3NodeLayout();
        CorfuRuntime runtime = getRuntime(l).connect();
        String[] serverArray = runtime.getLayoutView().getLayout().getAllServers()
                .toArray(new String[l.getAllServers().size()]);

        l.setRuntime(runtime);
        l.setEpoch(l.getEpoch() + 1);
        l.moveServersToEpoch();
        runtime.getLayoutView().updateLayout(l, 1L);

        assertThat(routerMap.get(serverArray[0]).getEpoch()).isEqualTo(1L);
        assertThat(routerMap.get(serverArray[1]).getEpoch()).isEqualTo(1L);
        assertThat(routerMap.get(serverArray[2]).getEpoch()).isEqualTo(1L);

        // Simulate router creation failure for the first endpoint in the list.
        failedNode.set((String) l.getAllServers().toArray()[0]);

        runtime.invalidateLayout();
        runtime.getLayoutView().getLayout();
        assertThat(routerMap.get(serverArray[0]).getEpoch()).isEqualTo(1L);
        assertThat(routerMap.get(serverArray[1]).getEpoch()).isEqualTo(2L);
        assertThat(routerMap.get(serverArray[2]).getEpoch()).isEqualTo(2L);

    }
}
