package org.corfudb.universe.scenario;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForLayoutChange;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;

public class OneNodePausedIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after one node paused
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Pause one node (hang the jvm process)
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by resuming the paused node
     * 5) Verify layout, cluster status and data path again
     */
    @Test(timeout = 300000)
    public void oneNodePausedTest() {
        getScenario().describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable<String, String> table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            testCase.it("Should pause one node and then resume", data -> {
                CorfuServer server2 = corfuCluster.getServerByIndex(2);

                // Pause one node and wait for layout's unresponsive servers to change
                // We choose the node with largest endpoint to prevent jittering
                // TODO: rotate the node to pause to fully test jittering and deadlock cases
                server2.pause();
                waitForLayoutChange(layout -> layout.getUnresponsiveServers()
                        .equals(Collections.singletonList(server2.getEndpoint())), corfuClient);

                // Verify cluster status is DEGRADED with one node down
                ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.DEGRADED);
                Map<String, NodeStatus> statusMap = clusterStatusReport.getClientServerConnectivityStatusMap();
                assertThat(statusMap.get(server2.getEndpoint())).isEqualTo(NodeStatus.DOWN);

                // Verify data path working fine
                for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }

                // Resume the stopped node and wait for layout's unresponsive servers to change
                server2.resume();
                waitForUnresponsiveServersChange(size -> size == 0, corfuClient);

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
            });
        });
    }
}
