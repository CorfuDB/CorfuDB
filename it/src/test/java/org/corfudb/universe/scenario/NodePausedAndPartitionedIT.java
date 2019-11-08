package org.corfudb.universe.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForClusterDown;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.ScenarioUtils.waitUninterruptibly;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

public class NodePausedAndPartitionedIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after one paused and another node partitioned
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Pause one node
     * 3) Symmetrically partition one node
     * 4) Verify layout, cluster status and data path
     * 5) Recover cluster by restart the stopped node and fix partition
     * 5) Verify layout, cluster status and data path
     */
    @Test(timeout = 300000)
    public void nodesPausedAndPartitionedTest() {
        workflow(wf -> {
            wf.deploy();

            CorfuCluster corfuCluster = wf.getUniverse()
                    .getGroup(wf.getFixture().data().getGroupParamByIndex(0).getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable<String, String> table = corfuClient
                    .createDefaultCorfuTable(DEFAULT_STREAM_NAME);

            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            //Should pause one node and partition another
            CorfuServer server0 = corfuCluster.getServerByIndex(0);
            CorfuServer server1 = corfuCluster.getServerByIndex(1);
            CorfuServer server2 = corfuCluster.getServerByIndex(2);

            // Pause one node and partition another one
            server1.pause();
            server2.disconnect(Arrays.asList(server0, server1));

            waitUninterruptibly(Duration.ofSeconds(20));

            // Verify cluster status
            corfuClient.invalidateLayout();
            ClusterStatusReport clusterStatusReport = corfuClient
                    .getManagementView()
                    .getClusterStatus();
            assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

            // Wait for failure detector finds cluster is down before recovering
            waitForClusterDown(table);

            // Recover cluster by resuming the paused node, removing
            // partition and wait for layout's unresponsive servers to change
            server1.resume();
            server2.reconnect(Arrays.asList(server0, server1));
            waitForUnresponsiveServersChange(size -> size == 0, corfuClient);

            waitUninterruptibly(Duration.ofSeconds(30));

            // Verify cluster status is STABLE
            corfuClient.invalidateLayout();
            clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
            assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

            // Verify data path working fine
            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
            }

            corfuClient.shutdown();
        });
    }
}
