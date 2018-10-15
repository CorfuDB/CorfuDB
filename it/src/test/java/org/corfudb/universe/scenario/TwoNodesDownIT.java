package org.corfudb.universe.scenario;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import static org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForClusterDown;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;

public class TwoNodesDownIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after two nodes down
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop two nodes
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by sequentially restarting stopped nodes
     * 5) Verify layout, cluster status and data path again
     */
    @Test(timeout = 300000)
    public void twoNodesDownTest() {
        getScenario().describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            testCase.it("Should stop two nodes and then restart", data -> {
                CorfuServer server0 = corfuCluster.getNode("node9000");
                CorfuServer server1 = corfuCluster.getNode("node9001");
                CorfuServer server2 = corfuCluster.getNode("node9002");

                // Sequentially stop two nodes
                server1.stop(Duration.ofSeconds(10));
                server2.stop(Duration.ofSeconds(10));

                // Verify cluster status is UNAVAILABLE with two node down and one node up
                corfuClient.invalidateLayout();
                ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                // TODO: There is a bug with ClusterStatus, see issue #1482, uncomment after patch
                // assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.UNAVAILABLE);
                Map<String, NodeStatus> statusMap = clusterStatusReport.getClientServerConnectivityStatusMap();
                assertThat(statusMap.get(server0.getEndpoint())).isEqualTo(NodeStatus.UP);
                assertThat(statusMap.get(server1.getEndpoint())).isEqualTo(NodeStatus.DOWN);
                assertThat(statusMap.get(server2.getEndpoint())).isEqualTo(NodeStatus.DOWN);

                // Wait for failure detector finds cluster is down before recovering
                waitForClusterDown(table);

                // Sequentially restart two nodes and wait for layout's unresponsive servers to change
                server1.start();
                server2.start();
                waitForUnresponsiveServersChange(size -> size == 0, corfuClient);

                // Verify cluster status is STABLE
                clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

                // Verify data path working fine
                for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }
            });
        });
    }
}
