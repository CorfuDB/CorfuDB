package org.corfudb.universe.scenario;

import org.corfudb.runtime.RebootUtil;
import org.corfudb.runtime.RebootUtil.RebootUtilWrapper;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.BaseHandler;
import org.corfudb.runtime.clients.LayoutHandler;
import org.corfudb.runtime.clients.ManagementHandler;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.UniverseManager.UniverseWorkflow;
import org.corfudb.universe.group.cluster.docker.DockerCorfuCluster;
import org.corfudb.universe.node.client.LocalCorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;

public class AddNodeDuringInvalidateLayoutIT extends GenericIntegrationTest {

    /**
     * Test add node behavior while the layout is being invalidated concurrently.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster.
     * 2) Remove a node.
     * 2) Try resetting a node and pruning the routers at the same time. This should throw an exception.
     * 3) Create a separate router for resets. Try resetting a node and pruning the routers at the same.
     *    The reset should go through.
     * 4) Create a separate router for adding a node, and invoke the add node procedure.
     *    Keep pruning a router every time RPC is called. The add node should succeed.
     * 5) Verify that the node is present in the new layout.
     */
    @Test(timeout = 300000)
    public void AddNodeDuringInvalidateLayoutTest() {
        workflow(wf -> {
                    wf.deploy();
                    try {
                        addNodeDuringInvalidateLayout(wf);
                    } catch (Exception e) {
                        Assertions.fail("Test failed: " + e);
                    }
                }
        );
    }

    private void addNodeDuringInvalidateLayout(UniverseWorkflow<Fixture<UniverseParams>> wf) throws Exception {
        UniverseParams params = wf.getFixture().data();
        DockerCorfuCluster corfuCluster = wf.getUniverse()
                .getGroup(params.getGroupParamByIndex(0).getName());
        CorfuServer first = corfuCluster.getFirstServer();
        String endpoint = first.getEndpoint();
        LocalCorfuClient client = corfuCluster.getLocalCorfuClient();
        final Duration timeOutDuration = Duration.ofSeconds(5);
        final Duration pollDuration = Duration.ofSeconds(1);
        final int numRetries = 3;
        final int waitForResetDurationSeconds = 8;
        final int resetWaitPeriod = 2000;
        // Remove a node so we can add it back later.
        client.getRuntime().getManagementView()
                .removeNode(endpoint, numRetries, timeOutDuration, pollDuration);
        Layout layout = client.getLayout();
        assertThat(layout.getAllServers().contains(endpoint)).isFalse();
        // Create a base client to the node we want to reset and the spy to cause a race condition.
        BaseClient baseClient = client.getRuntime().getLayoutView()
                .getRuntimeLayout().getBaseClient(endpoint);
        BaseClient baseClientSpy = spy(baseClient);

        // When a base client tries to perform a reset, prune the routers.
        // This will prune the router of the base client.
        Mockito.doAnswer(invoke -> {
            client.getRuntime().pruneRouters(layout);
            return invoke.callRealMethod();
        }).when(baseClientSpy).reset();

        // When we try to issue a reset, depending on when the router was pruned,
        // either a TimeoutException will be thrown
        // (because the connection future times out),
        // or a NetworkException will be thrown
        // (because the connection future completed exceptionally).
        assertThatThrownBy(() -> CFUtils.getUninterruptibly(baseClientSpy.reset(),
                TimeoutException.class, NetworkException.class))
                .isInstanceOfAny(NetworkException.class, TimeoutException.class);

        // Now lets create a client router that is not part of a router pool,
        // issue a reset to the node we would want to add.
        // When a reset is called, the layout will be invalidated.
        RebootUtilWrapper rebootWrapper = new RebootUtilWrapper();
        RebootUtilWrapper spy = spy(rebootWrapper);
        Mockito.doAnswer(invoke -> {
            client.getRuntime().pruneRouters(layout);
            return invoke.callRealMethod();
        }).when(spy).reset(Matchers.anyObject(), Matchers.anyObject(), Matchers.anyInt(), Matchers.anyObject());

        int resetNodeRetryTimes = 5;
        Duration resetNodeRetryInterval = Duration.ofSeconds(5);

        assertThatCode(() -> spy.reset(endpoint, client.getRuntime().getParameters(),
                resetNodeRetryTimes, resetNodeRetryInterval)).doesNotThrowAnyException();

        // Wait until a node fully restarts.
        Sleep.sleepUninterruptibly(Duration.ofMillis(resetWaitPeriod));

        // Now lets create a client router that is not part of a router pool,
        // and run the add node workflow.
        // Every time a clientRouter sends a request, the layout will be invalidated.
        try (NettyClientRouter clientRouter = new NettyClientRouter(NodeLocator.parseString(endpoint),
                client.getRuntime().getParameters())) {

            NettyClientRouter spyRouter = spy(clientRouter);

            spyRouter.addClient(new LayoutHandler())
                    .addClient(new ManagementHandler())
                    .addClient(new BaseHandler());

            Mockito.doAnswer(invoke -> {
                client.getRuntime().pruneRouters(layout);
                return invoke.callRealMethod();
            }).when(spyRouter).sendMessageAndGetCompletable(Mockito.any(), Mockito.any());

            // Run add node.
            client.getManagementView()
                    .addNode(endpoint, numRetries, timeOutDuration, pollDuration, spyRouter);
            // Verify that the node was added.
            assertThat(client.getLayout().getAllServers()).contains(endpoint);
        }
    }
}
