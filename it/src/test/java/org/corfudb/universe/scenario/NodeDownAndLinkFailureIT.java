package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.runtime.view.ClusterStatusReport.ClusterStatus;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.corfudb.universe.scenario.ScenarioUtils.*;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER;

@Slf4j
public class NodeDownAndLinkFailureIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after one node down and one link failure.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Stop one node
     * 3) Create a link failure between two nodes which
     *    results in a partial partition
     * 4) Restart the stopped node
     * 5) Verify layout, cluster status and data path
     * 6) Remove the link failure
     * 7) Verify layout, cluster status and data path again
     */
    @Test(timeout = 300000)
    public void nodeDownAndLinkFailureTest() {
        getScenario().describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            CorfuTable<String, String> table = corfuClient.createDefaultCorfuTable(DEFAULT_STREAM_NAME);
            for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            testCase.it("Should fail one link then one node and then heal", data -> {
                CorfuServer server0 = corfuCluster.getServerByIndex(0);
                CorfuServer server1 = corfuCluster.getServerByIndex(1);
                CorfuServer server2 = corfuCluster.getServerByIndex(2);

                log.info("Stop server2 and wait for layout's unresponsive servers to change");
                server2.stop(Duration.ofSeconds(10));
                waitForNextEpoch(corfuClient);
                assertThat(corfuClient.getLayout().getUnresponsiveServers()).containsExactly(server2.getEndpoint());

                // Create link failure between server0 and server1
                // After this, cluster becomes unavailable.
                // NOTE: cannot use waitForClusterDown() since the partition only happens on server side, client
                // can still connect to two nodes, write to table so system down handler will not be triggered.
                log.info("Create link failure between server0 and server1");
                server0.disconnect(Collections.singletonList(server1));

                // Restart the stopped node, server0 and server1 still partitioned,
                // wait for the one with larger endpoint be marked as unresponsive.
                log.info("Restart the stopped node, server0 and server1 still partitioned wait for " +
                        "the one with larger endpoint be marked as unresponsive.");
                server2.start();
                String serverToKick = Collections.max(Arrays.asList(server0.getEndpoint(), server1.getEndpoint()));

                log.info("Wait until epoch is updated two times");
                waitForNextEpoch(corfuClient);
                waitForNextEpoch(corfuClient);
                assertThat(corfuClient.getLayout().getUnresponsiveServers())
                        .as("Incorrect unresponsive servers: %s", corfuClient.getLayout().getUnresponsiveServers())
                        .containsExactly(serverToKick);

                // Cluster status should be DEGRADED after one node is marked unresponsive
                //ClusterStatusReport clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                // TODO: uncomment the following line after ClusterStatus API is fixed for partial partition
                // assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.DEGRADED);
                // TODO: add node status check after we redefine NodeStatus semantics

                log.info("Repair the partition between server0 and server1");
                server0.reconnect(Collections.singletonList(server1));
                waitForNextEpoch(corfuClient);
                Layout layout = corfuClient.getLayout();
                assertThat(layout.getUnresponsiveServers())
                        .as("Wrong layout: %s. Unresponsive servers: %s",
                                layout.getLayoutServers(), layout.getUnresponsiveServers()
                        )
                        .hasSize(0);

                final Duration sleepDuration = Duration.ofSeconds(1);
                // Verify cluster status is STABLE
                /*clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                while (!clusterStatusReport.getClusterStatus().equals(ClusterStatus.STABLE)) {
                    clusterStatusReport = corfuClient.getManagementView().getClusterStatus();
                    Sleep.sleepUninterruptibly(sleepDuration);
                }
                assertThat(clusterStatusReport.getClusterStatus()).isEqualTo(ClusterStatus.STABLE);
*/
                // Verify data path working fine
                for (int i = 0; i < DEFAULT_TABLE_ITER; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }
            });
        });
    }
}
