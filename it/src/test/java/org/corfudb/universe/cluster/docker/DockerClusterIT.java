package org.corfudb.universe.cluster.docker;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerInfo;
import org.corfudb.universe.Universe;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.scenario.fixture.Fixtures.ClusterFixture;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.cluster.Cluster.ClusterParams;
import static org.corfudb.universe.node.CorfuServer.ServerParams;
import static org.corfudb.universe.service.Service.ServiceParams;

public class DockerClusterIT {
    private static final Universe UNIVERSE = Universe.getInstance();

    private final DockerClient docker;
    private DockerCluster dockerCluster;

    public DockerClusterIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @After
    public void tearDown() {
        dockerCluster.shutdown();
    }

    @Test
    public void deploySingleServiceSingleNodeTest() throws Exception {
        Fixtures.MultipleServersFixture serversFixture = Fixtures.MultipleServersFixture.builder().numNodes(1).build();
        Fixtures.CorfuServiceFixture serviceFixture = Fixtures.CorfuServiceFixture.builder().servers(serversFixture).build();
        ClusterFixture clusterFixture = ClusterFixture.builder().service(serviceFixture).build();

        ClusterParams clusterParams = clusterFixture.data();
        dockerCluster = UNIVERSE
                .buildDockerCluster(clusterParams, docker)
                .deploy();

        ServerParams serverParam = clusterParams
                .getService(serviceFixture.getServiceName(), ServerParams.class)
                .getNodes()
                .get(0);

        ContainerInfo container = docker.inspectContainer(serverParam.getGenericName());
        assertThat(container.state().running()).isTrue();
        assertThat(container.name()).isEqualTo("/" + serverParam.getGenericName());
        assertThat(container.networkSettings().networks().get(clusterParams.getNetworkName())).isNotNull();
    }

    @Test
    public void deploySingleServiceMultipleNodesTest() throws Exception {
        ClusterFixture clusterFixture = ClusterFixture.builder().build();

        //setup
        final ClusterParams clusterParams = clusterFixture.data();
        dockerCluster = UNIVERSE
                .buildDockerCluster(clusterParams, docker)
                .deploy();

        ServiceParams<ServerParams> serviceParams = clusterParams
                .getService(clusterFixture.getService().getServiceName(), ServerParams.class);

        for (ServerParams serverParams : serviceParams.getNodes()) {
            ContainerInfo container = docker.inspectContainer(serverParams.getGenericName());
            assertThat(container.state().running()).isTrue();
            assertThat(container.name()).isEqualTo("/" + serverParams.getGenericName());
            assertThat(container.networkSettings().networks().get(clusterParams.getNetworkName())).isNotNull();
        }
    }
}