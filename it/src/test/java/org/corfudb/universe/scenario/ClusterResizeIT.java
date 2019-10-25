package org.corfudb.universe.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.universe.Universe.UniverseMode;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class ClusterResizeIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after add/remove nodes
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially remove two nodes from cluster
     * 3) Verify layout and data path
     * 4) Sequentially add two nodes back into cluster
     * 5) Verify layout and data path again
     */
    @Test(timeout = 300000)
    public void clusterResizeTest() {
        workflow(wf -> {
            wf.deploy();
            UniverseParams params = wf.getFixture().data();

            ClientParams clientFixture = ClientParams.builder().build();

            CorfuCluster corfuCluster = wf.getUniverse()
                    .getGroup(params.getGroupParamByIndex(0).getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable<String, String> table =
                    corfuClient.createDefaultCorfuTable(TestFixtureConst.DEFAULT_STREAM_NAME);

            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            List<CorfuServer> servers = Arrays.asList(
                    corfuCluster.getServerByIndex(1),
                    corfuCluster.getServerByIndex(2)
            );

            //should remove two nodes from corfu cluster
            {
                CorfuServer server0 = corfuCluster.getFirstServer();

                // Sequentially remove two nodes from cluster
                for (CorfuServer candidate : servers) {
                    corfuClient.getManagementView().removeNode(
                            candidate.getEndpoint(),
                            clientFixture.getNumRetry(),
                            clientFixture.getTimeout(),
                            clientFixture.getPollPeriod()
                    );
                }

                // Reset all nodes so that we do not end up with an OverwriteException.
                for (CorfuServer candidate : servers) {
                    corfuClient.getRuntime().getLayoutView().getRuntimeLayout()
                            .getBaseClient(candidate.getEndpoint()).reset();
                }

                // Verify layout contains only the node that is not removed
                corfuClient.invalidateLayout();
                assertThat(corfuClient.getLayout().getAllServers())
                        .containsExactly(server0.getEndpoint());

                // Verify data path working fine
                for (int x = 0; x < TestFixtureConst.DEFAULT_TABLE_ITER; x++) {
                    assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
                }

                if (wf.getUniverseMode() == UniverseMode.VM) {
                    ScenarioUtils.waitUninterruptibly(Duration.ofSeconds(15));
                }
            }

            //should add two nodes back to corfu cluster
            {

                // Sequentially add two nodes back into cluster
                for (CorfuServer candidate : servers) {
                    corfuClient.getManagementView().addNode(
                            candidate.getEndpoint(),
                            clientFixture.getNumRetry(),
                            clientFixture.getTimeout(),
                            clientFixture.getPollPeriod()
                    );
                }

                // Verify layout should contain all three nodes
                corfuClient.invalidateLayout();
                assertThat(corfuClient.getLayout().getAllServers().size())
                        .isEqualTo(corfuCluster.nodes().size());

                // Verify data path working fine
                for (int x = 0; x < TestFixtureConst.DEFAULT_TABLE_ITER; x++) {
                    assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
                }
            }

            corfuClient.shutdown();
        });
    }
}
