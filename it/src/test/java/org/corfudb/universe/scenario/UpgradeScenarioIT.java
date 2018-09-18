package org.corfudb.universe.scenario;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.group.CorfuCluster;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.LocalCorfuClient;
import org.corfudb.universe.universe.Universe;
import org.junit.After;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.scenario.fixture.Fixtures.UniverseFixture;

public class UpgradeScenarioIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    private final DockerClient docker;
    private Universe universe;

    private final Duration timeout = Duration.ofMinutes(1);
    private final Duration pollPeriod = Duration.ofMillis(50);
    private final int workflowNumRetry = 3;

    private final long executorTimeout = 60;
    private final TimeUnit executorTimeUnit = TimeUnit.SECONDS;

    public UpgradeScenarioIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        if (universe != null) {
            universe.shutdown();
        }
    }

    @Test
    public void clusterResizeTest() {
        // Deploy a default three node fixture
        UniverseFixture universeFixture = UniverseFixture.builder().build();
        final int corfuTableValues = 1000;

        universe = UNIVERSE_FACTORY
                .buildDockerCluster(universeFixture.data(), docker)
                .deploy();

        Scenario<Universe.UniverseParams, UniverseFixture> scenario = Scenario.with(universeFixture);

        scenario.describe((fixture, testCase) -> {
            CorfuCluster corfuCluster = universe.getGroup(universeFixture.getGroup().getGroupName());
            CorfuServer mainServer = corfuCluster.getNode("node9000");

            LocalCorfuClient corfuClient = LocalCorfuClient.builder()
                    .serverParams(mainServer.getParams())
                    .build()
                    .deploy();

            CorfuTable table = corfuClient.getObjectsView()
                    .build()
                    .setType(CorfuTable.class)
                    .setStreamName("table1")
                    .open();

            testCase.it("should check the cluster size", data -> {
                Layout layout = corfuClient.getLayout();
                assertThat(layout.getAllActiveServers().size()).isEqualTo(corfuCluster.nodes().size());
            });

            testCase.it("should check corfu table", data -> {
                for (int i = 0; i < corfuTableValues; i++) {
                    table.put(String.valueOf(i), String.valueOf(i));
                }

                for (int i = 0; i < corfuTableValues; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }
            });

            testCase.it("should remove two nodes from corfu cluster", data -> {
                for (int i = 1; i <= 2; i++) {
                    CorfuServer candidate = corfuCluster.getNode("node" + (9000 + i));
                    corfuClient.remove(candidate);
                }
                Layout layout = corfuClient.getLayout();

                assertThat(layout.getAllServers().size()).isEqualTo(1);
                for (int x = 0; x < corfuTableValues; x++) {
                    assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
                }
            });

            testCase.it("should add two nodes back to corfu cluster", data -> {

                for (int i = 1; i <= 2; i++) {
                    CorfuServer candidate = corfuCluster.getNode("node" + (9000 + i));
                    corfuClient.add(candidate);
                }
                Layout layout = corfuClient.getLayout();
                assertThat(layout.getAllActiveServers().size()).isEqualTo(corfuCluster.nodes().size());

                for (int x = 0; x < corfuTableValues; x++) {
                    assertThat(table.get(String.valueOf(x))).isEqualTo(String.valueOf(x));
                }
            });

            // Test hands of god
            testCase.it("Should force remove two nodes from cluster", data -> {

                CorfuServer server1 = corfuCluster.getNode("node9001");
                CorfuServer server2 = corfuCluster.getNode("node9002");
                server1.kill();
                server2.kill();

                corfuClient.getManagementView()
                        .forceRemoveNode(server1.getParams().getEndpoint(), workflowNumRetry, timeout, pollPeriod);
                corfuClient.getManagementView()
                        .forceRemoveNode(server2.getParams().getEndpoint(), workflowNumRetry, timeout, pollPeriod);
                corfuClient.invalidateLayout();

                Layout layout = corfuClient.getLayout();
                assertThat(layout.getAllActiveServers().size()).isEqualTo(1);

                for (int i = 0; i < corfuTableValues; i++) {
                    assertThat(table.get(String.valueOf(i))).isEqualTo(String.valueOf(i));
                }
            });

        });
    }
}
