package org.corfudb.infrastructure;

import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.view.Layout;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.protocols.service.CorfuProtocolLayout.getBootstrapLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getBootstrapManagementRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getReportFailureRequestMsg;

/**
 * Checks the various services and messages handled by the
 * Management Server.
 * <p>
 * Created by zlokhandwala on 11/2/16.
 */
public class ManagementServerTest extends AbstractServerTest {

    private ManagementServer managementServer;

    @Override
    public ManagementServer getDefaultServer() {
        // Adding layout server for management server runtime to connect to.
        ServerContext serverContext = new ServerContextBuilder()
                .setSingle(false)
                .setPort(SERVERS.PORT_0)
                .setServerRouter(getRouter())
                .build();
        // Required for management server to fetch layout.
        router.addServer(new LayoutServer(serverContext));
        router.addServer(new BaseServer(serverContext));
        // Required to fetch global tails while handling failures.
        // Required for management server to bootstrap during initialization.
        router.addServer(new SequencerServer(serverContext));
        router.setServerContext(serverContext);
        managementServer = new ManagementServer(serverContext,
                new ManagementServer.ManagementServerInitializer());

        return managementServer;
    }

    @After
    public void cleanUp() {
        managementServer.shutdown();
    }

    /**
     * Testing the status of the failure detector and shutdown functionality.
     */
    @Test
    public void checkFailureDetectorStatus() {
        assertThat(managementServer.getManagementAgent().getRemoteMonitoringService()
                .getFailureDetectorWorker().isShutdown()).isFalse();
        managementServer.shutdown();
        assertThat(managementServer.getManagementAgent().getRemoteMonitoringService()
                .getFailureDetectorWorker().isShutdown()).isTrue();
    }

    /**
     * Bootstrapping the management server multiple times.
     */
    @Test
    public void bootstrapManagementServer() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        sendRequest(getBootstrapLayoutRequestMsg(layout), ClusterIdCheck.IGNORE, EpochCheck.IGNORE);

        CompletableFuture<Boolean> future = sendRequestWithClusterId(
                getBootstrapManagementRequestMsg(layout), layout.getClusterId(), ClusterIdCheck.IGNORE, EpochCheck.IGNORE);

        assertThat(future.join()).isEqualTo(true);

        future = sendRequestWithClusterId(
                getBootstrapManagementRequestMsg(layout), layout.getClusterId(), ClusterIdCheck.IGNORE, EpochCheck.IGNORE);

        assertThatThrownBy(future::join).hasCauseExactlyInstanceOf(AlreadyBootstrappedException.class);
    }

    /**
     * Triggering the failure handler with and without bootstrapping the server.
     */
    @Test
    public void triggerFailureHandler() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        CompletableFuture<Boolean> future = sendRequest(
                getBootstrapLayoutRequestMsg(layout), ClusterIdCheck.IGNORE, EpochCheck.IGNORE);

        assertThat(future.join()).isEqualTo(true);

        future = sendRequestWithClusterId(
                getReportFailureRequestMsg(0L, Collections.emptySet()),
                layout.getClusterId(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);

        assertThatThrownBy(future::join).hasCauseExactlyInstanceOf(NoBootstrapException.class);

        future = sendRequestWithClusterId(
                getBootstrapManagementRequestMsg(layout), layout.getClusterId(), ClusterIdCheck.IGNORE, EpochCheck.IGNORE);

        assertThat(future.join()).isEqualTo(true);

        future = sendRequestWithClusterId(
                getReportFailureRequestMsg(0L, Collections.emptySet()),
                layout.getClusterId(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);

        assertThat(future.join()).isEqualTo(true);
    }
}
