package org.corfudb.infrastructure;

import org.corfudb.infrastructure.configuration.ServerConfigurator;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.DetectorMsg;
import org.corfudb.protocols.wireprotocol.LayoutBootstrapRequest;
import org.corfudb.runtime.view.Layout;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.RejectedExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        ServerConfigurator serverConfigurator = new ServerConfigurator(serverContext);
        // Required for management server to bootstrap during initialization.
        router.addServer(new SequencerServer(serverContext));
        managementServer = serverConfigurator.getManagementServer();
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
        sendMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(layout)));
        sendMessage(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST.payloadMsg(layout));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);
        sendMessage(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST.payloadMsg(layout));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.MANAGEMENT_ALREADY_BOOTSTRAP_ERROR);
    }

    /**
     * Triggering the failure handler with and without bootstrapping the server.
     */
    @Test
    public void triggerFailureHandler() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        sendMessage(CorfuMsgType.LAYOUT_BOOTSTRAP.payloadMsg(new LayoutBootstrapRequest(layout)));
        sendMessage(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED.payloadMsg(
                new DetectorMsg(0L, Collections.emptySet(), Collections.emptySet())));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR);
        sendMessage(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST.payloadMsg(layout));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);
        sendMessage(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED.payloadMsg(
                new DetectorMsg(0L, Collections.emptySet(), Collections.emptySet())));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);
    }

    /**
     * Shutting down the management server's executor and test whether the heartbeats can go through.
     */
    @Test
    public void testHeartbeatSeparateThread() {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        managementServer.getExecutor(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED).shutdownNow();
        assertThatThrownBy(() -> sendMessage(CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST.payloadMsg(layout)))
                .isInstanceOf(RejectedExecutionException.class);
        sendMessage(CorfuMsgType.NODE_STATE_REQUEST.msg());
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.NODE_STATE_RESPONSE);
    }
}
