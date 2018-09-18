package org.corfudb.universe.universe.docker;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerInfo;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.group.CorfuCluster.CorfuClusterParams;
import org.corfudb.universe.scenario.fixture.Fixtures.CorfuGroupFixture;
import org.corfudb.universe.scenario.fixture.Fixtures.MultipleServersFixture;
import org.corfudb.universe.scenario.fixture.Fixtures.UniverseFixture;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.group.Group.GroupParams;
import static org.corfudb.universe.node.CorfuServer.ServerParams;
import static org.corfudb.universe.universe.Universe.UniverseParams;

public class DockerUniverseIT {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    private final DockerClient docker;
    private DockerUniverse dockerCluster;

    public DockerUniverseIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    /**
     * Shutdown the {@link org.corfudb.universe.universe.Universe} after test completion
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
    public void deploySingleGroupSingleNodeTest() throws Exception {
        MultipleServersFixture serversFixture = MultipleServersFixture.builder().numNodes(1).build();
        CorfuGroupFixture groupFixture = CorfuGroupFixture.builder().servers(serversFixture).build();
        UniverseFixture universeFixture = UniverseFixture.builder().group(groupFixture).build();

        UniverseParams universeParams = universeFixture.data();
        dockerCluster = UNIVERSE_FACTORY
                .buildDockerCluster(universeParams, docker)
                .deploy();

        ServerParams serverParam = universeParams
                .getGroupParams(groupFixture.getGroupName(), CorfuClusterParams.class)
                .getNodesParams()
                .get(0);

        ContainerInfo container = docker.inspectContainer(serverParam.getName());
        assertThat(container.state().running()).isTrue();
        assertThat(container.name()).isEqualTo("/" + serverParam.getName());
        assertThat(container.networkSettings().networks().get(universeParams.getNetworkName())).isNotNull();
    }

    /**
     * Deploy a service then deploy three corfu server nodes then add all of them into the service.
     *
     * @throws Exception an error
     */
    @Test
    public void deploySingleGroupMultipleNodesTest() throws Exception {
        UniverseFixture universeFixture = UniverseFixture.builder().build();

        //setup
        final UniverseParams universeParams = universeFixture.data();
        dockerCluster = UNIVERSE_FACTORY
                .buildDockerCluster(universeParams, docker)
                .deploy();

        GroupParams groupParams = universeParams
                .getGroupParams(universeFixture.getGroup().getGroupName(), CorfuClusterParams.class);

        for (ServerParams serverParams : groupParams.<ServerParams>getNodesParams()) {
            ContainerInfo container = docker.inspectContainer(serverParams.getName());
            assertThat(container.state().running()).isTrue();
            assertThat(container.name()).isEqualTo("/" + serverParams.getName());
            assertThat(container.networkSettings().networks().get(universeParams.getNetworkName())).isNotNull();
        }
    }
}