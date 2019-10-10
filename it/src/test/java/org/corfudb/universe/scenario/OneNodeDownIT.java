package org.corfudb.universe.scenario;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;

public class OneNodeDownIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after one node down.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Stop one node
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by restarting the stopped node
     * 5) Verify layout, cluster status and data path again
     */
    @Test(timeout = 300000)
    public void oneNodeDownTest() {

        getScenario().describe((fixture, testCase) -> {

        });
    }
}
