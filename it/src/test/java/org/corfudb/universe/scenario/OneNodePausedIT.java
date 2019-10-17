package org.corfudb.universe.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
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

            //Should pause one node and then resume
            CorfuServer server1 = corfuCluster.getServerByIndex(1);

            // Pause one node and wait for layout's unresponsive servers to change
            server1.pause();
            waitForUnresponsiveServersChange(size -> size == 1, corfuClient);

            // Verify layout, unresponsive servers should contain only one node
            Layout layout = corfuClient.getLayout();
            assertThat(layout.getUnresponsiveServers())
                    .containsExactly(server1.getEndpoint());

            // Verify cluster status is DEGRADED with one node down
            ClusterStatusReport clusterStatusReport = corfuClient.getManagementView()
                    .getClusterStatus();
            assertThat(clusterStatusReport.getClusterStatus())
                    .isEqualTo(ClusterStatus.DEGRADED);
            Map<String, NodeStatus> statusMap = clusterStatusReport
                    .getClusterNodeStatusMap();
            assertThat(statusMap.get(server1.getEndpoint())).isEqualTo(NodeStatus.DOWN);

            // Verify data path working fine
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
            }

            // Resume the stopped node and wait for layout's unresponsive servers to change
            server1.resume();
            waitForUnresponsiveServersChange(size -> size == 0, corfuClient);

            final Duration sleepDuration = Duration.ofSeconds(1);
            // Verify cluster status is STABLE
            clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
            while (!clusterStatusReport.getClusterStatus().equals(ClusterStatus.STABLE)) {
                clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                Sleep.sleepUninterruptibly(sleepDuration);
            }
            assertThat(clusterStatusReport.getClusterStatus())
                    .isEqualTo(ClusterStatus.STABLE);

            // Verify data path working fine
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
            }

            corfuClient.shutdown();
        });
    }
}
