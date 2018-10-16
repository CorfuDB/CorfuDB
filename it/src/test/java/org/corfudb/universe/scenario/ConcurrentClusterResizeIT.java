package org.corfudb.universe.scenario;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForLayoutServersChange;

public class ConcurrentClusterResizeIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after add/remove nodes concurrently
     * <p>
     * 1) Deploy and bootstrap a five nodes cluster
     * 2) Concurrently remove four nodes from cluster
     * 3) Verify layout and data path
     * 4) Concurrently add four nodes back into cluster
     * 5) Verify layout and data path again
     */
    @Test(timeout = 600000)
    public void concurrentClusterResizeTest() {
        // Deploy a five nodes cluster
        final int numNodes = 5;

        getScenario(numNodes).describe((fixture, testCase) -> {
            ClientParams clientFixture = fixture.getClient();
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable table = corfuClient.createDefaultCorfuTable(TestFixtureConst.DEFAULT_STREAM_NAME);
            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            // Get the servers list to be added/removed
            List<CorfuServer> servers = IntStream.range(1, numNodes)
                    .mapToObj(i -> (CorfuServer) corfuCluster.getNode("node" + (9000 + i)))
                    .collect(Collectors.toList());

            testCase.it("should concurrently remove four nodes from cluster", data -> {
                CorfuServer server0 = corfuCluster.getNode("node9000");

                // Concurrently remove four nodes from cluster
                ExecutorService executor = Executors.newFixedThreadPool(numNodes - 1);

                servers.forEach(node -> executor.submit(() -> corfuClient.getManagementView().removeNode(
                        node.getEndpoint(),
                        clientFixture.getNumRetry(),
                        clientFixture.getTimeout(),
                        clientFixture.getPollPeriod())
                ));

                // Wait for layout servers to change
                waitForLayoutServersChange(size -> size == 1, corfuClient);
                executor.shutdownNow();

                // Verify layout contains only one node
                corfuClient.invalidateLayout();
                assertThat(corfuClient.getLayout().getAllServers()).containsExactly(server0.getEndpoint());

                // Verify data path working fine
                for (int x = 0; x < TestFixtureConst.DEFAULT_TABLE_ITER; x++) {
                    assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
                }
            });

            testCase.it("should concurrently add four nodes back into cluster", data -> {
                // Concurrently add four nodes back into cluster and wait for cluster to stabilize
                ExecutorService executor = Executors.newFixedThreadPool(numNodes - 1);
                servers.forEach(node -> executor.submit(() -> corfuClient.getManagementView().addNode(
                        node.getEndpoint(),
                        clientFixture.getNumRetry(),
                        clientFixture.getTimeout(),
                        clientFixture.getPollPeriod())
                ));

                // Wait for layout servers to change
                waitForLayoutServersChange(size -> size == numNodes, corfuClient);
                executor.shutdownNow();

                // Verify data path working fine
                for (int x = 0; x < TestFixtureConst.DEFAULT_TABLE_ITER; x++) {
                    assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
                }
            });
        });
    }
}
