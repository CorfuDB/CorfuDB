package org.corfudb.universe.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForClusterDown;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForLayoutChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatusReliability;
import org.corfudb.runtime.view.ClusterStatusReport.ConnectivityStatus;
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

public class TwoNodesDownIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after two nodes down.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially stop two nodes
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by sequentially restarting stopped nodes
     * 5) Verify layout, cluster status and data path again
     */
    @Test(timeout = 300000)
    public void twoNodesDownTest() {
        workflow(wf -> {
            wf.deploy();

            CorfuCluster corfuCluster = wf.getUniverse()
                    .getGroup(wf.getFixture().data().getGroupParamByIndex(0).getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable<String, String> table = corfuClient
                    .createDefaultCorfuTable(DEFAULT_STREAM_NAME);

            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            //Should stop two nodes and then restart
            CorfuServer server0 = corfuCluster.getServerByIndex(0);
            CorfuServer server1 = corfuCluster.getServerByIndex(1);
            CorfuServer server2 = corfuCluster.getServerByIndex(2);

            // Sequentially stop two nodes
            server1.stop(Duration.ofSeconds(10));
            server2.stop(Duration.ofSeconds(10));

            // Verify cluster status is UNAVAILABLE with two node down and one node up
            corfuClient.invalidateLayout();
            ClusterStatusReport clusterStatusReport = corfuClient
                    .getManagementView()
                    .getClusterStatus();

            Map<String, NodeStatus> nodeStatusMap = clusterStatusReport.getClusterNodeStatusMap();
            Map<String, ConnectivityStatus> connectivityStatusMap = clusterStatusReport
                    .getClientServerConnectivityStatusMap();
            ClusterStatusReliability reliability = clusterStatusReport.getClusterStatusReliability();

            assertThat(connectivityStatusMap.get(server0.getEndpoint()))
                    .isEqualTo(ConnectivityStatus.RESPONSIVE);
            assertThat(connectivityStatusMap.get(server1.getEndpoint()))
                    .isEqualTo(ConnectivityStatus.UNRESPONSIVE);
            assertThat(connectivityStatusMap.get(server2.getEndpoint()))
                    .isEqualTo(ConnectivityStatus.UNRESPONSIVE);

            assertThat(nodeStatusMap.get(server0.getEndpoint())).isEqualTo(NodeStatus.NA);
            assertThat(nodeStatusMap.get(server1.getEndpoint())).isEqualTo(NodeStatus.NA);
            assertThat(nodeStatusMap.get(server2.getEndpoint())).isEqualTo(NodeStatus.NA);

            assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.UNAVAILABLE);
            assertThat(reliability).isEqualTo(ClusterStatusReliability.WEAK_NO_QUORUM);

            // Wait for failure detector finds cluster is down before recovering
            waitForClusterDown(table);

            // Sequentially restart two nodes and wait for layout's unresponsive servers to change
            server1.start();
            server2.start();

            Layout initialLayout = clusterStatusReport.getLayout();
            waitForLayoutChange(layout -> layout.getEpoch() > initialLayout.getEpoch()
                    && layout.getUnresponsiveServers().size() == 0, corfuClient);

            final Duration sleepDuration = Duration.ofSeconds(1);

            // Verify cluster status is STABLE
            clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
            while (!clusterStatusReport.getClusterStatus().equals(ClusterStatus.STABLE)) {
                clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                Sleep.sleepUninterruptibly(sleepDuration);
            }
            assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

            // Verify data path working fine
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
            }

            corfuClient.shutdown();
        });
    }
}
