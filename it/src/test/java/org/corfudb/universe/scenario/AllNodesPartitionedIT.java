package org.corfudb.universe.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForClusterUp;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.ScenarioUtils.waitUninterruptibly;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.ConnectivityStatus;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
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
    @Test(timeout = 300000)
    public void allNodesPartitionedTest() {
        workflow(wf -> {
            wf.deploy();

            UniverseParams params = wf.getFixture().data();

            CorfuCluster<CorfuServer, CorfuClusterParams> corfuCluster = wf.getUniverse()
                    .getGroup(params.getGroupParamByIndex(0).getName());

            CorfuClusterParams corfuClusterParams = corfuCluster.getParams();

            assertThat(corfuCluster.nodes().size()).isEqualTo(3);
            assertThat(corfuCluster.nodes().size()).isEqualTo(corfuClusterParams.size());

            assertThat(corfuCluster.getParams().getNodesParams().size())
                    .as("Invalid cluster: %s, but expected 3 nodes",
                            corfuClusterParams.getClusterNodes()
                    )
                    .isEqualTo(3);

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable<String, String> table =
                    corfuClient.createDefaultCorfuTable(TestFixtureConst.DEFAULT_STREAM_NAME);
            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }


            // Symmetrically partition all nodes and wait for failure
            // detector to work and cluster to stabilize
            List<CorfuServer> allServers = corfuCluster.<CorfuServer>nodes().values().asList();
            allServers.forEach(server -> {
                List<CorfuServer> otherServers = new ArrayList<>(allServers);
                otherServers.remove(server);
                server.disconnect(otherServers);
            });

            waitUninterruptibly(Duration.ofSeconds(20));

            // Verify cluster and node status
            ClusterStatusReport clusterStatusReport = corfuClient
                    .getManagementView()
                    .getClusterStatus();
            assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

            Map<String, NodeStatus> statusMap = clusterStatusReport.getClusterNodeStatusMap();
            corfuCluster.nodes()
                    .values()
                    .forEach(node ->
                            assertThat(statusMap.get(node.getEndpoint())).isEqualTo(NodeStatus.UP)
                    );

            Map<String, ConnectivityStatus> connectivityMap = clusterStatusReport
                    .getClientServerConnectivityStatusMap();

            corfuCluster.nodes().values().forEach(node -> {
                assertThat(connectivityMap.get(node.getEndpoint()))
                        .isEqualTo(ConnectivityStatus.RESPONSIVE);
            });

            // Remove partitions and wait for layout's unresponsive servers to change
            waitUninterruptibly(Duration.ofSeconds(10));
            corfuCluster.nodes().values().forEach(CorfuServer::reconnect);

            waitForUnresponsiveServersChange(size -> size == 0, corfuClient);

            // Verify cluster status is STABLE
            clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
            assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

            waitForClusterUp(table, "0");

            // Verify data path working fine
            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
            }


            corfuClient.shutdown();

        });
    }
}
