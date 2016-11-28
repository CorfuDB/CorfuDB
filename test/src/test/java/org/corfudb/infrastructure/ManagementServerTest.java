package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.FailureDetectorMsg;
import org.corfudb.runtime.view.Layout;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
        ServerContext serverContext = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(getRouter())
                .build();
        managementServer = new ManagementServer(serverContext);
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
        ManagementServer managementServer = getDefaultServer();

        assertThat(!managementServer.getFailureDetectorService().isShutdown());
        managementServer.shutdown();
        assertThat(managementServer.getFailureDetectorService().isShutdown());
    }

    /**
     * Bootstrapping the management server multiple times.
     */
    @Test
    public void bootstrapManagementServer() {
        Layout layout = TestLayoutBuilder.single(9000);
        sendMessage(CorfuMsgType.MANAGEMENT_BOOTSTRAP.payloadMsg(layout));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);
        sendMessage(CorfuMsgType.MANAGEMENT_BOOTSTRAP.payloadMsg(layout));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.MANAGEMENT_ALREADY_BOOTSTRAP);
    }

    /**
     * Triggering the failure handler with and without bootstrapping the server.
     */
    @Test
    public void triggerFailureHandler() {
        Layout layout = TestLayoutBuilder.single(9000);
        Map<String, Boolean> map = new HashMap<>();
        map.put("key", true);
        sendMessage(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED.payloadMsg(new FailureDetectorMsg(map)));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP);
        sendMessage(CorfuMsgType.MANAGEMENT_BOOTSTRAP.payloadMsg(layout));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);
        sendMessage(CorfuMsgType.MANAGEMENT_FAILURE_DETECTED.payloadMsg(new FailureDetectorMsg(map)));
        assertThat(getLastMessage().getMsgType()).isEqualTo(CorfuMsgType.ACK);
    }
}
