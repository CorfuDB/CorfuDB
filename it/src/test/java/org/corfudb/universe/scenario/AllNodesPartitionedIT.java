package org.corfudb.universe.scenario;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;

public class AllNodesPartitionedIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after all nodes are partitioned symmetrically
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Symmetrically partition all nodes so that they can't communicate
     * to any other node in cluster and vice versa
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by reconnecting the partitioned node
     * 5) Verify layout, cluster status and data path again
     */
    @Ignore("Fix iptables for travis")
    @Test(timeout = 300000)
    public void allNodesPartitionedTest() {
        getScenario().describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable table = corfuClient.createDefaultCorfuTable(TestFixtureConst.DEFAULT_STREAM_NAME);
            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            testCase.it("Should partition all nodes and then recover", data -> {
                // Symmetrically partition all nodes and wait for failure
                // detector to work and cluster to stabilize
                corfuCluster.<CorfuServer>nodes().values().forEach(CorfuServer::disconnect);

                // Verify cluster status is UNAVAILABLE with all nodes UP
                // TODO: There is a bug in NodeStatus API, waiting for patch and uncomment following lines
                ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                // assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.UNAVAILABLE);
                //
                // Map<String, NodeStatus> statusMap = clusterStatusReport.getClientServerConnectivityStatusMap();
                // corfuCluster.<CorfuServer>nodes().values().forEach(node -> {
                //     CorfuServerParams serverParams = node.getParams();
                //     assertThat(statusMap.get(serverParams.getEndpoint())).isEqualTo(NodeStatus.UP);
                // });
                //
                // // Wait for failure detector finds cluster is down before recovering
                // waitForClusterDown(table);

                // Remove partitions and wait for layout's unresponsive servers to change
                corfuCluster.<CorfuServer>nodes().values().forEach(CorfuServer::reconnect);
                waitForUnresponsiveServersChange(size -> size == 0, corfuClient);

                // Verify cluster status is STABLE
                clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

                // Verify data path working fine
                for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }
            });
        });
    }
}
