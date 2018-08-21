package org.corfudb.universe.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.Universe;
import org.corfudb.universe.cluster.Cluster;
import org.corfudb.universe.cluster.docker.DockerCluster;
import org.corfudb.universe.cluster.docker.FakeDns;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.universe.node.CorfuServer.Mode;
import static org.corfudb.universe.node.CorfuServer.Persistence;
import static org.corfudb.universe.node.CorfuServer.ServerParams;

public class DockerServiceIT {
    private static final Universe UNIVERSE = Universe.getInstance();
    private static final FakeDns FAKE_DNS = FakeDns.getInstance();

    private static final String LOCALHOST = "127.0.0.1";
    private static final String STREAM_NAME = "stream";

    private final DockerClient docker;
    private DockerCluster dockerCluster;

    public DockerServiceIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @Before
    public void setUp(){
        FAKE_DNS.install();
    }

    @After
    public void tearDown() {
        dockerCluster.shutdown();
    }

    @Test
    public void corfuServerSingleServiceSingleNodeTest() throws Exception {
        ServerParams serverParams = deployment();
        FAKE_DNS.addForwardResolution(serverParams.getGenericName(), InetAddress.getByName(LOCALHOST));

        CorfuRuntime node1Runtime = new CorfuRuntime(serverParams.getEndpoint())
                .setCacheDisabled(true)
                .connect();

        CorfuTable<String, String> table = node1Runtime
                .getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(STREAM_NAME)
                .open();

        final int size = 10;
        for (int i = 0; i < size ; i++) {
            table.put("key" + i, "value" + i);
        }

        assertThat(table.size()).isEqualTo(size);
    }

    @Test
    public void corfuServerSingleServiceMultipleNodesTest() throws Exception {
        final int nodes = 3;

        List<ServerParams> serversParams = new ArrayList<>();

        for (int i = 0; i < nodes; i++) {
            Mode mode = i == 0 ? Mode.SINGLE : Mode.CLUSTER;
            final int port = 9000 + i;

            ServerParams serverParam = ServerParams.builder()
                    .mode(mode)
                    .logDir("/tmp/")
                    .logLevel(Level.TRACE)
                    .persistence(Persistence.MEMORY)
                    .host("node" + port)
                    .port(port)
                    .build();

            serversParams.add(serverParam);
            FAKE_DNS.addForwardResolution(serverParam.getGenericName(), InetAddress.getByName(LOCALHOST));
        }

        Service.ServiceParams<ServerParams> serviceParams = Service.ServiceParams.<ServerParams>builder()
                .name("corfuServer")
                .nodes(ImmutableList.copyOf(serversParams))
                .build();

        Cluster.ClusterParams clusterParams = Cluster.ClusterParams.builder()
                .services(ImmutableMap.of(serviceParams.getName(), serviceParams))
                .networkName("CorfuNet" + UUID.randomUUID().toString())
                .build();

        dockerCluster = UNIVERSE
                .buildDockerCluster(clusterParams, docker)
                .deploy();


        CorfuRuntime node1Runtime = new CorfuRuntime(serversParams.get(0).getEndpoint())
                .setCacheDisabled(true)
                .connect();

        final Duration timeout = Duration.ofMinutes(5);
        final Duration pollPeriod = Duration.ofMillis(50);
        final int workflowNumRetry = 3;

        for (int i =1; i < serviceParams.getNodes().size(); i++) {
            ServerParams server = serversParams.get(i);
            node1Runtime
                    .getManagementView()
                    .addNode(server.getEndpoint(), workflowNumRetry, timeout, pollPeriod);
        }

        node1Runtime.invalidateLayout();
        final int clusterSize = serviceParams.getNodes().size();
        assertThat(node1Runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSize);

    }

    private ServerParams deployment() {
        ServerParams serverParam = ServerParams.builder()
                .mode(Mode.SINGLE)
                .logDir("/tmp")
                .logLevel(Level.TRACE)
                .persistence(Persistence.MEMORY)
                .host("node9000")
                .port(9000)
                .build();


        Service.ServiceParams<ServerParams> serviceParams = Service.ServiceParams.<ServerParams>builder()
                .name("corfuServer")
                .nodes(ImmutableList.of(serverParam))
                .build();

        Cluster.ClusterParams clusterParams = Cluster.ClusterParams.builder()
                .services(ImmutableMap.of(serviceParams.getName(), serviceParams))
                .networkName("CorfuNet" + UUID.randomUUID().toString())
                .build();

        dockerCluster = UNIVERSE
                .buildDockerCluster(clusterParams, docker)
                .deploy();

        return serverParam;
    }

}