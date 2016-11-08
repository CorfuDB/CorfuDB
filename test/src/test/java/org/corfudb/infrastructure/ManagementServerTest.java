package org.corfudb.infrastructure;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the various services and messages handled by the
 * Management Server.
 * <p>
 * Created by zlokhandwala on 11/2/16.
 */
public class ManagementServerTest extends AbstractServerTest {

    @Override
    public ManagementServer getDefaultServer() {
        ServerContext serverContext = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(getRouter())
                .build();
        return new ManagementServer(serverContext);
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
}
