package org.corfudb.universe.cluster.docker;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerInfo;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.scenario.fixture.Fixtures.ClusterFixture;
import org.corfudb.universe.scenario.fixture.Fixtures.CorfuServiceFixture;
import org.corfudb.universe.scenario.fixture.Fixtures.MultipleServersFixture;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.cluster.Cluster.ClusterParams;
import static org.corfudb.universe.node.CorfuServer.ServerParams;
import static org.corfudb.universe.service.Group.GroupParams;

public class DockerClusterIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    private final DockerClient docker;
    private DockerCluster dockerCluster;

    public DockerClusterIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    /**
     * Shutdown the cluster after test completion
     */
    @After
    public void tearDown() {
        dockerCluster.shutdown();
    }

    /**
     * Deploy a single service then deploy a single corfu server and add the server into the service
     * @throws Exception an error
     */
    @Test
    public void deploySingleServiceSingleNodeTest() throws Exception {
        MultipleServersFixture serversFixture = MultipleServersFixture.builder().numNodes(1).build();
        CorfuServiceFixture serviceFixture = CorfuServiceFixture.builder().servers(serversFixture).build();
        ClusterFixture clusterFixture = ClusterFixture.builder().service(serviceFixture).build();

        ClusterParams clusterParams = clusterFixture.data();
        dockerCluster = UNIVERSE_FACTORY
                .buildDockerCluster(clusterParams, docker)
                .deploy();

        ServerParams serverParam = clusterParams
                .getServiceParams(serviceFixture.getServiceName(), ServerParams.class)
                .getNodeParams()
                .get(0);

        ContainerInfo container = docker.inspectContainer(serverParam.getName());
        assertThat(container.state().running()).isTrue();
        assertThat(container.name()).isEqualTo("/" + serverParam.getName());
        assertThat(container.networkSettings().networks().get(clusterParams.getNetworkName())).isNotNull();
    }

    /**
     * Deploy a service then deploy three corfu server nodes then add all of them into the service.
     *
     * @throws Exception an error
     */
    @Test
    public void deploySingleServiceMultipleNodesTest() throws Exception {
        ClusterFixture clusterFixture = ClusterFixture.builder().build();

        //setup
        final ClusterParams clusterParams = clusterFixture.data();
        dockerCluster = UNIVERSE_FACTORY
                .buildDockerCluster(clusterParams, docker)
                .deploy();

        GroupParams<ServerParams> groupParams = clusterParams
                .getServiceParams(clusterFixture.getService().getServiceName(), ServerParams.class);

        for (ServerParams serverParams : groupParams.getNodeParams()) {
            ContainerInfo container = docker.inspectContainer(serverParams.getName());
            assertThat(container.state().running()).isTrue();
            assertThat(container.name()).isEqualTo("/" + serverParams.getName());
            assertThat(container.networkSettings().networks().get(clusterParams.getNetworkName())).isNotNull();
        }
    }
}