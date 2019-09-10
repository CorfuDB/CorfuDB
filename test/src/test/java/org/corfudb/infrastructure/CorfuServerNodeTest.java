package org.corfudb.infrastructure;

import com.codahale.metrics.MetricRegistry;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.runtime.CorfuRuntime;

import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;


public class CorfuServerNodeTest extends AbstractCorfuTest {

    @Test
    public void testCorfuServerNodeStart() {

        // Start the Corfu Server
        ServerContext context = new ServerContextBuilder()
            .setAddress("localhost").setImplementation("auto").build();
        MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();
        CorfuServerNode serverNode = new CorfuServerNode(context, metricRegistry);
        serverNode.start();

        // Create CorfuRuntime and connect to the server
        CorfuRuntime runtime = CorfuRuntime.fromParameters(
            buildCorfuRuntimeParameters());
        String connectionString = "localhost:9000";
        runtime.parseConfigurationString(connectionString);
        runtime.connect();

        // print the layout servers
        assertEquals(1, runtime.getLayoutServers().size());
        assertEquals(connectionString, runtime.getLayoutServers().get(0));
    }

    private CorfuRuntime.CorfuRuntimeParameters buildCorfuRuntimeParameters() {
        return CorfuRuntime.CorfuRuntimeParameters.builder()
            .useFastLoader(false)
            .cacheDisabled(true)
            .priorityLevel(PriorityLevel.HIGH)
            .build();
    }
}