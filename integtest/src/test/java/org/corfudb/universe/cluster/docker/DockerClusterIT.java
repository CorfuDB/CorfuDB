package org.corfudb.universe.cluster.docker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerInfo;
import org.corfudb.universe.Universe;
import org.junit.After;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.cluster.Cluster.ClusterParams;
import static org.corfudb.universe.node.CorfuServer.Persistence;
import static org.corfudb.universe.node.CorfuServer.Mode;
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
    public void tearDown(){
        dockerCluster.shutdown();
    }

    @Test
    public void deploySingleServiceSingleNodeTest() throws Exception{
        ServerParams serverParam = ServerParams.builder()
                .mode(Mode.SINGLE)
                .logDir("/tmp/log/")
                .logLevel(Level.TRACE)
                .persistence(Persistence.MEMORY)
                .host("localhost")
                .port(9000)
                .build();

        ServiceParams<ServerParams> serviceParams = ServiceParams.<ServerParams>builder()
                .name("corfuServer")
                .nodes(ImmutableList.of(serverParam))
                .build();

        ClusterParams clusterParams = ClusterParams.builder()
                .services(ImmutableMap.of(serviceParams.getName(), serviceParams))
                .networkName("CorfuNet" + UUID.randomUUID().toString())
                .build();

        dockerCluster = UNIVERSE
                .buildDockerCluster(clusterParams, docker)
                .deploy();

        ContainerInfo container = docker.inspectContainer(serverParam.getGenericName());
        assertThat(container.state().running()).isTrue();
        assertThat(container.name()).isEqualTo("/" + serverParam.getGenericName());
        assertThat(container.networkSettings().networks().get(clusterParams.getNetworkName())).isNotNull();
    }

    @Test
    public void deploySingleServiceMultipleNodesTest() throws Exception{
        final int nodes = 3;

        List<ServerParams> serversParams = new ArrayList<>();

        for (int i = 0; i < nodes; i++) {
            Mode mode = i == 0 ? Mode.SINGLE : Mode.CLUSTER;
            final int port = 9000 + i;

            ServerParams serverParam = ServerParams.builder()
                    .mode(mode)
                    .logDir("/tmp/")
                    .logLevel(Level.TRACE)
                    .persistence(Persistence.DISK)
                    .host("node" + port)
                    .port(port)
                    .build();

            serversParams.add(serverParam);
        }

        ServiceParams<ServerParams> serviceParams = ServiceParams.<ServerParams>builder()
                .name("corfuServer")
                .nodes(ImmutableList.copyOf(serversParams))
                .build();

        ClusterParams clusterParams = ClusterParams.builder()
                .services(ImmutableMap.of(serviceParams.getName(), serviceParams))
                .networkName("CorfuNet" + UUID.randomUUID().toString())
                .build();

        dockerCluster = UNIVERSE
                .buildDockerCluster(clusterParams, docker)
                .deploy();

        for (ServerParams serverParams : serviceParams.getNodes()) {
            ContainerInfo container = docker.inspectContainer(serverParams.getGenericName());
            assertThat(container.state().running()).isTrue();
            assertThat(container.name()).isEqualTo("/" + serverParams.getGenericName());
            assertThat(container.networkSettings().networks().get(clusterParams.getNetworkName())).isNotNull();
        }
    }
}