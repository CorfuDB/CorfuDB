package org.corfudb.universe.service;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.Universe;
import org.corfudb.universe.cluster.docker.DockerCluster;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.scenario.Scenario;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.cluster.Cluster.ClusterParams;
import static org.corfudb.universe.node.CorfuServer.Mode;
import static org.corfudb.universe.scenario.Fixtures.ClusterFixture;
import static org.corfudb.universe.scenario.Fixtures.CorfuServiceFixture;
import static org.corfudb.universe.scenario.Fixtures.MultipleServersFixture;

public class DockerServiceIT {
    private static final Universe UNIVERSE = Universe.getInstance();
    private static final String STREAM_NAME = "stream";

    private final DockerClient docker;
    private DockerCluster dockerCluster;

    public DockerServiceIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        dockerCluster.shutdown();
    }

    @Test
    public void corfuServerSingleServiceSingleNodeTest() {
        MultipleServersFixture serversFixture = MultipleServersFixture.builder().numNodes(1).build();
        CorfuServiceFixture serviceFixture = CorfuServiceFixture.builder().servers(serversFixture).build();
        ClusterFixture clusterFixture = ClusterFixture.builder().service(serviceFixture).build();

        dockerCluster = UNIVERSE
                .buildDockerCluster(clusterFixture.data(), docker)
                .deploy();

        Scenario<ClusterParams, ClusterFixture> scenario = Scenario.with(clusterFixture);
        scenario.describe((fixture, testCase) -> {
            final int size = 10;

            CorfuRuntime node1Runtime = new CorfuRuntime(serversFixture.data().get(0).getEndpoint())
                    .setCacheDisabled(true)
                    .connect();

            CorfuTable<String, String> table = node1Runtime
                    .getObjectsView()
                    .build()
                    .setType(CorfuTable.class)
                    .setStreamName(STREAM_NAME)
                    .open();

            for (int i = 0; i < size; i++) {
                table.put("key" + i, "value" + i);
            }
            int tableSize = table.size();
            assertThat(tableSize).isEqualTo(size);
        });
    }

    @Test
    public void addAndRemoveCorfuServerIntoTheClusterTest() {
        ClusterFixture clusterFixture = ClusterFixture.builder().build();

        //setup
        dockerCluster = UNIVERSE
                .buildDockerCluster(clusterFixture.data(), docker)
                .deploy();

        Scenario<ClusterParams, ClusterFixture> scenario = Scenario
                .with(ClusterFixture.builder().build());

        scenario.describe((fixture, testCase) -> {
            ClusterParams clusterParams = fixture.data();
            String serviceName = clusterParams.getServices()
                    .values()
                    .asList()
                    .get(0)
                    .getName();

            Service service = dockerCluster.getService(serviceName);

            testCase.it("check cluster size", Integer.class, test -> test
                    .action(data -> service.nodes().size())
                    .check((data, corfuServers) ->
                            assertThat(corfuServers).isEqualTo(fixture.getService().getServers().getNumNodes())
                    )
            );

            CorfuServer mainServer = service.<CorfuServer>nodes().get(0);
            final int clusterSize = service.getParams().getNodes().size();
            testCase.it("should add node", Layout.class, test -> test
                    .action(data -> {
                        mainServer.connectCorfuRuntime();

                        for (CorfuServer corfuServer : service.<CorfuServer>nodes()) {
                            if (corfuServer.getParams().getMode() == Mode.SINGLE) {
                                continue;
                            }
                            mainServer.addNode(corfuServer);
                        }

                        return mainServer.getLayout().get();
                    }).check((data, layout) -> {
                        assertThat(layout.getAllActiveServers().size()).isEqualTo(clusterSize);
                    })
            );

            testCase.it("should remove node from cluster", Layout.class, test -> test
                    .action(data -> {
                        CorfuServer secondServer = service.<CorfuServer>nodes().get(1);
                        mainServer.removeNode(secondServer);
                        return mainServer.getLayout().get();
                    }).check((data, layout) -> {
                        assertThat(layout.getAllActiveServers().size()).isEqualTo(clusterSize - 1);
                    })
            );
        });
    }
}