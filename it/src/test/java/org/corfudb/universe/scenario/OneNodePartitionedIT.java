package org.corfudb.universe.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.junit.Ignore;
import org.junit.Test;

public class OneNodePartitionedIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after one node partitioned symmetrically
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Symmetrically partition one node so that it can't communicate
     * to any other node in cluster and vice versa
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by reconnecting the partitioned node
     * 5) Verify layout, cluster status and data path again
     */
    @Ignore("Fix disconnect method in DockerCorfuServer. Wrong cluster status (UNAVAILABLE) on VM")
    @Test(timeout = 300000)
    public void oneNodeSymmetricPartitionTest() {
        getScenario().describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            testCase.it("should symmetrically partition one node from cluster", data -> {
                CorfuServer server1 = corfuCluster.getNode("node9001");

                // Symmetrically Partition one node and wait for layout's unresponsive servers to change
                server1.disconnect();
                waitForUnresponsiveServersChange(size -> size == 1, corfuClient);

                // Verify layout, unresponsive servers should contain only one node
                Layout layout = corfuClient.getLayout();
                assertThat(layout.getUnresponsiveServers()).containsExactly(server1.getEndpoint());

                // Verify cluster status is DEGRADED with all nodes UP
                ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.DEGRADED);
                // TODO: There is a bug in NodeStatus API, waiting for patch and uncomment following lines
                // Map<String, NodeStatus> statusMap = clusterStatusReport.getClientServerConnectivityStatusMap();
                // assertThat(statusMap.get(server1.getParams().getEndpoint())).isEqualTo(NodeStatus.UP);

                // Verify data path working fine
                for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }

                // Remove partition and wait for layout's unresponsive servers to change
                server1.reconnect();
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
