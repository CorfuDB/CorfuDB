package org.corfudb.universe.scenario;

import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.ClusterStatusReport.NodeStatus;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.UniverseManager.UniverseWorkflow;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class RepeatingFailureIT extends GenericIntegrationTest {

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
    public void repeatingNodeFailreTest() {

        workflow(wf -> {
            wf.setupDocker(fixture -> {
                fixture.getUniverse().cleanUpEnabled(false);
            });

            wf.deploy();

            try {
                repeatingNodeFailre(wf);
            } catch (Exception e) {
                fail("Test failed", e);
            }
        });
    }

    private void repeatingNodeFailre(UniverseWorkflow<Fixture<UniverseParams>> wf) throws Exception {
        String groupName = wf.getFixture().data().getGroupParamByIndex(0).getName();
        CorfuCluster corfuCluster = wf.getUniverse().getGroup(groupName);

        CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

        ICorfuTable<String, String> table = corfuClient
                .createDefaultCorfuTable(DEFAULT_STREAM_NAME);

        for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
            table.insert(String.valueOf(i), String.valueOf(i));
        }

        //Should stop one node and then restart
        for (int i = 0; i < 3; i++) {
            corfuServerRestart(corfuCluster, corfuClient);
        }
        assertEquals(
                1,
                corfuClient.getLayout().getHealProbes().size(),
                () -> "Wrong probes section. Layout: " + corfuClient.getLayout()
        );

        ClusterStatusReport clusterStatusReport;

        final Duration sleepDuration = Duration.ofSeconds(1);
        // Verify cluster status is STABLE
        clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
        while (!clusterStatusReport.getClusterStatus().equals(ClusterStatus.STABLE)) {
            clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
            TimeUnit.MILLISECONDS.sleep(sleepDuration.toMillis());
        }
        assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

        // Verify data path working fine
        for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
            assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
        }
    }

    private static void corfuServerRestart(CorfuCluster corfuCluster, CorfuClient corfuClient) {
        CorfuServer server0 = corfuCluster.getFirstServer();

        // Stop one node and wait for layout's unresponsive servers to change
        server0.stop(Duration.ofSeconds(10));
        waitForUnresponsiveServersChange(size -> size == 1, corfuClient);

        // Verify layout, unresponsive servers should contain only the stopped node
        Layout layout = corfuClient.getLayout();
        assertThat(layout.getUnresponsiveServers()).containsExactly(server0.getEndpoint());

        // Verify cluster status is DEGRADED with one node down
        ClusterStatusReport clusterStatusReport = corfuClient
                .getManagementView()
                .getClusterStatus();
        assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.DEGRADED);

        Map<String, NodeStatus> statusMap = clusterStatusReport.getClusterNodeStatusMap();
        assertThat(statusMap.get(server0.getEndpoint())).isEqualTo(NodeStatus.DOWN);

        // restart the stopped node and wait for layout's unresponsive servers to change
        server0.start();
        waitForUnresponsiveServersChange(size -> size == 0, corfuClient);
    }
}
