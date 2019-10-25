package org.corfudb.universe.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.junit.Test;

import java.time.Duration;

public class HandOfGodIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after killing and force removing nodes
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Kill two nodes
     * 3) Force remove the dead nodes (Hand of God)
     * 4) Verify layout, cluster status and data path
     */
    @Test(timeout = 300000)
    public void handOfGodTest() {
        workflow(wf -> {
            wf.deploy();

            ClientParams clientFixture = ClientParams.builder().build();
            CorfuCluster corfuCluster = wf.getUniverse()
                    .getGroup(wf.getFixture().data().getGroupParamByIndex(0).getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable<String, String> table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            //Should force remove two nodes from cluster
            CorfuServer server0 = corfuCluster.getServerByIndex(0);
            CorfuServer server1 = corfuCluster.getServerByIndex(1);
            CorfuServer server2 = corfuCluster.getServerByIndex(2);

            // Sequentially kill two nodes
            server1.kill();
            server2.kill();

            // Force remove the dead nodes
            corfuClient.getManagementView().forceRemoveNode(
                    server1.getEndpoint(),
                    clientFixture.getNumRetry(),
                    clientFixture.getTimeout(),
                    clientFixture.getPollPeriod()
            );

            corfuClient.getManagementView().forceRemoveNode(
                    server2.getEndpoint(),
                    clientFixture.getNumRetry(),
                    clientFixture.getTimeout(),
                    clientFixture.getPollPeriod()
            );

            // Verify layout contains only the node that is up
            corfuClient.invalidateLayout();
            Layout layout = corfuClient.getLayout();
            assertThat(layout.getAllActiveServers()).containsExactly(server0.getEndpoint());

            // Verify cluster status is STABLE
            ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
            assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);

            ScenarioUtils.waitUninterruptibly(Duration.ofSeconds(20));

            // Verify data path working
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
            }

            corfuClient.shutdown();
        });
    }
}
