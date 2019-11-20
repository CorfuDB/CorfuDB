package org.corfudb.universe.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

public class OneLinkFailureIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after one link failure.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Create a link failure between two nodes which
     * results in a partial partition
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by removing the link failures
     * 5) Verify layout, cluster status and data path again
     */
    @Test(timeout = 300000)
    public void oneLinkFailureTest() {
        workflow(wf -> {
            wf.deploy();

            CorfuCluster corfuCluster = wf.getUniverse()
                    .getGroup(wf.getFixture().data().getGroupParamByIndex(0).getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable<String, String> table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            //Should fail one link and then heal"
            CorfuServer server0 = corfuCluster.getServerByIndex(0);
            CorfuServer server2 = corfuCluster.getServerByIndex(2);

            // Create link failure between server0 and server2
            server0.disconnect(Collections.singletonList(server2));
            // Server0 and server2 has same number of link failure ie. 1, the one with
            // larger endpoint should be marked as unresponsive.
            String serverToKick = Collections.max(
                    Arrays.asList(server0.getEndpoint(), server2.getEndpoint())
            );
            waitForUnresponsiveServersChange(size -> size == 1, corfuClient);

            assertThat(corfuClient.getLayout().getUnresponsiveServers())
                    .containsExactly(serverToKick);

            // Cluster status should be DEGRADED after one node is marked unresponsive
            ClusterStatusReport clusterStatusReport = corfuClient
                    .getManagementView()
                    .getClusterStatus();
            assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.DEGRADED);

            // Verify data path working fine
            ScenarioUtils.waitUninterruptibly(Duration.ofSeconds(10));
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
            }

            // Repair the partition between server0 and server2
            server0.reconnect(Collections.singletonList(server2));
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

            corfuClient.shutdown();
        });
    }
}
