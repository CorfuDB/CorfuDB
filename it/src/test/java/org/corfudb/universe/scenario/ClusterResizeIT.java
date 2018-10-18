package org.corfudb.universe.scenario;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;

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
        getScenario().describe((fixture, testCase) -> {
            final int numNodes = fixture.getNumNodes();
            ClientParams clientFixture = fixture.getClient();

            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable table = corfuClient.createDefaultCorfuTable(TestFixtureConst.DEFAULT_STREAM_NAME);

            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            testCase.it("should remove two nodes from corfu cluster", data -> {
                CorfuServer server0 = corfuCluster.getNode("node9000");

                // Sequentially remove two nodes from cluster
                for (int i = 1; i <= numNodes - 1; i++) {
                    CorfuServer candidate = corfuCluster.getNode("node" + (9000 + i));
                    corfuClient.getManagementView().removeNode(
                            candidate.getEndpoint(),
                            clientFixture.getNumRetry(),
                            clientFixture.getTimeout(),
                            clientFixture.getPollPeriod()
                    );
                }

                // Verify layout contains only the node that is not removed
                corfuClient.invalidateLayout();
                assertThat(corfuClient.getLayout().getAllServers()).containsExactly(server0.getEndpoint());

                // Verify data path working fine
                for (int x = 0; x < TestFixtureConst.DEFAULT_TABLE_ITER; x++) {
                    assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
                }
            });

            testCase.it("should add two nodes back to corfu cluster", data -> {
                // Sequentially add two nodes back into cluster
                for (int i = 1; i <= numNodes - 1; i++) {
                    CorfuServer candidate = corfuCluster.getNode("node" + (9000 + i));
                    corfuClient.getManagementView().addNode(
                            candidate.getEndpoint(),
                            clientFixture.getNumRetry(),
                            clientFixture.getTimeout(),
                            clientFixture.getPollPeriod()
                    );
                }

                // Verify layout should contain all three nodes
                corfuClient.invalidateLayout();
                assertThat(corfuClient.getLayout().getAllServers().size()).isEqualTo(corfuCluster.nodes().size());

                // Verify data path working fine
                for (int x = 0; x < TestFixtureConst.DEFAULT_TABLE_ITER; x++) {
                    assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
                }
            });
        });
    }
}
