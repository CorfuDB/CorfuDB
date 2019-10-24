package org.corfudb.universe.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.ScenarioUtils.waitForLayoutServersChange;

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

        workflow(wf -> {
            wf.setupDocker(fixture -> fixture.getCluster().numNodes(numNodes));
            wf.setupProcess(fixture -> fixture.getCluster().numNodes(numNodes));
            wf.setupVm(fixture -> fixture.getCluster().numNodes(numNodes));

            wf.deploy();

            ClientParams clientFixture = ClientParams.builder().build();
            CorfuCluster corfuCluster = wf.getUniverse()
                    .getGroup(wf.getFixture().data().getGroupParamByIndex(0).getName());

            assertThat(corfuCluster.nodes().size()).isEqualTo(numNodes);

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable<String, String> table =
                    corfuClient.createDefaultCorfuTable(TestFixtureConst.DEFAULT_STREAM_NAME);
            for (int i = 0; i < TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            CorfuServer server0 = corfuCluster.getFirstServer();

            // Get the servers list to be added/removed -all servers in the cluster exclude server0
            List<CorfuServer> servers = IntStream.range(1, numNodes)
                    .mapToObj(corfuCluster::getServerByIndex)
                    .collect(Collectors.toList());

            //should concurrently remove four nodes from cluster

            // Concurrently remove four nodes from cluster
            ExecutorService executor = Executors.newFixedThreadPool(numNodes - 1);

            servers.forEach(node -> {
                Runnable removeNodeAction = () -> corfuClient.getManagementView().removeNode(
                        node.getEndpoint(),
                        clientFixture.getNumRetry(),
                        clientFixture.getTimeout(),
                        clientFixture.getPollPeriod()
                );
                executor.submit(removeNodeAction);
            });

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

            //should concurrently add four nodes back into cluster"

            // Concurrently add four nodes back into cluster and wait for cluster to stabilize
            ExecutorService executor2 = Executors.newFixedThreadPool(numNodes - 1);
            servers.forEach(node -> executor2.submit(() -> corfuClient.getManagementView().addNode(
                    node.getEndpoint(),
                    clientFixture.getNumRetry(),
                    clientFixture.getTimeout(),
                    clientFixture.getPollPeriod())
            ));

            // Wait for layout servers to change
            waitForLayoutServersChange(size -> size == numNodes, corfuClient);
            executor2.shutdownNow();

            // Verify data path working fine
            for (int x = 0; x < TestFixtureConst.DEFAULT_TABLE_ITER; x++) {
                assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
            }

            corfuClient.shutdown();
        });
    }
}
