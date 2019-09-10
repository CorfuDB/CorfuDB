package org.corfudb.infrastructure;

import com.codahale.metrics.MetricRegistry;

import org.corfudb.runtime.CorfuRuntime;

import org.junit.jupiter.api.Test;


public class CorfuServerNodeTest {

    @Test
    public void testCorfuServerNodeStart() {
        ServerContext context = new ServerContextBuilder()
            .setAddress("localhost").build();
        MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();
        CorfuServerNode serverNode = new CorfuServerNode(context, metricRegistry);
        serverNode.startAndListen();
    }
}
