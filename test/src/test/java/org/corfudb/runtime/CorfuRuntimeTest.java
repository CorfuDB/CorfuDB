package org.corfudb.runtime;

import java.lang.reflect.Parameter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;
import org.corfudb.infrastructure.TestLayoutBuilder;

import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.clients.TestClientRouter;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.exceptions.unrecoverable.RuntimeShutdownError;
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
    static final long TIMEOUT_CORFU_RUNTIME_IN_MS = 100;



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
     * Tests if an asynchronous connection request properly times out.
     */
    @Test
    public void asyncConnectTimeout() throws Exception {
        // Create a runtime which does not automatically connect and
        // is set to a non-existent layout server.
        CorfuRuntime rt = CorfuRuntime.fromParameters(CorfuRuntimeParameters.builder()
                                        .autoConnect(false)
                                        .layoutServer(NodeLocator.builder()
                                                        .host("error")
                                                        .port(1)
                                                        .build())
                                        .build());
        try {
            // Wait TIMEOUT_SHORT for the connection to complete (it will never complete).
            // It should timeout and throw a timeout exception.
            assertThatThrownBy(() ->
                rt.connectAsync().get(PARAMETERS.TIMEOUT_SHORT.toMillis(), TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
        } finally {
            rt.shutdown();
        }
    }

    /**
     * Tests if an asynchronous connection request can succeeed.
     */
    @Test
    public void asyncConnectSucceeds() throws Exception {
        CorfuRuntime rt = getDefaultRuntime();
        try {
            // Wait TIMEOUT_LONG for the connection to complete
            rt.connectAsync().get(PARAMETERS.TIMEOUT_LONG.toMillis(), TimeUnit.MILLISECONDS);
            // Make sure that the instance is actually connected.
            assertThat(rt.getLayoutView().getLayout())
                .isNotNull();
        } finally {
            rt.shutdown();
        }
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

        // Ensure that we never (never is time_to_wait seconds here) get a new layout.
        CompletableFuture cf =
            CFUtils.within(CompletableFuture.supplyAsync(() -> {
                rt.invalidateLayout();
                return CFUtils.getUninterruptibly(rt.layout);
            }), Duration.ofSeconds(TIME_TO_WAIT_FOR_LAYOUT_IN_SEC));

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
            long localTimeStart;

            TimeoutHandler(CorfuRuntime rt, long maxTimeout) {
                this.rt = rt;
                this.maxTimeout = maxTimeout;
            }

            void startTimeout() {
                localTimeStart = System.currentTimeMillis();
            }

            void checkIfTimeout() {
                if (System.currentTimeMillis() - localTimeStart > maxTimeout) {
                    stopRuntimeAndThrowException();
                }
            }

            void stopRuntimeAndThrowException() {
                rt.shutdown();
                throw new SystemUnavailableError("Timeout " + maxTimeout + " elapsed");
            }
        }


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
                isInstanceOf(RuntimeShutdownError.class);

    }

}
