package org.corfudb.infrastructure;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.NodeLocator;

import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;


public class CorfuServerNodeTest extends AbstractCorfuTest {

    private final String connectionString = "localhost:9000";
    private final String hostname = "localhost";

    /**
     * Verifies that the Corfu runtime can successfully connect to a test Corfu
     * Server and has layout servers populated correctly.
     */
    @Test
    public void testCorfuServerNodeStart() {

        // Start the Corfu Server
        ServerContext context = new ServerContextBuilder()
            .setAddress(hostname).setImplementation("auto").build();
        CorfuServerNode serverNode = new CorfuServerNode(context);
        serverNode.start();

        // Create CorfuRuntime and connect to the server
        CorfuRuntime runtime = CorfuRuntime.fromParameters(
            buildCorfuRuntimeParameters());
        runtime.connect();

        // Verify that the localhost has been added as a layout server
        assertEquals(1, runtime.getLayoutServers().size());
        assertEquals(connectionString, runtime.getLayoutServers().get(0));

        // Shutdown
        runtime.shutdown();
        //serverNode.close();
    }

    private CorfuRuntime.CorfuRuntimeParameters buildCorfuRuntimeParameters() {
        NodeLocator locator = NodeLocator.parseString(connectionString);
        return CorfuRuntime.CorfuRuntimeParameters.builder()
            .layoutServer(locator)
            .useFastLoader(false)
            .cacheDisabled(true)
            .priorityLevel(PriorityLevel.HIGH)
            .build();
    }
}
