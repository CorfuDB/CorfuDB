package org.corfudb.integtest;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.Sleep;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@Slf4j
public class TestTestIT {
    private static final FakeDns FAKE_DNS = FakeDns.getInstance();
    private static final Random RND = new SecureRandom();

    private final DockerClient docker;

    private final Duration timeout = Duration.ofMinutes(5);
    private final Duration pollPeriod = Duration.ofMillis(50);
    private final int workflowNumRetry = 3;

    private final int nodes = 15;
    private final int degradation = 11;
    private final int scenarioRetry = 5;

    private final String streamName = "stream1";
    private final String networkName = "corfunetwork";

    public TestTestIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @BeforeClass
    public static void init(){
        FAKE_DNS.install();
    }

    @Test
    public void testTest() throws Exception {
        setupNetwork(networkName);

        Map<Integer, CorfuServerDockerized> servers = new HashMap<>();

        servers.put(9000, startServer(9000, networkName, ServerMode.SINGLE));
        FAKE_DNS.addForwardResolution("node" + 9000, InetAddress.getByName("127.0.0.1"));

        for (int i = 1; i < nodes; i++) {
            ServerMode serverMode = ServerMode.CLUSTER;
            int port = 9000 + i;

            servers.put(port, startServer(port, networkName, serverMode));
            FAKE_DNS.addForwardResolution("node" + port, InetAddress.getByName("127.0.0.1"));
        }

        CorfuRuntime node1Runtime = new CorfuRuntime(getConnectionString(9000)).setCacheDisabled(true).connect();

        CorfuTable<String, String> table = node1Runtime.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(streamName)
                .open();

        int clusterSize = 1;
        for (Integer port : servers.keySet()) {
            if(port == 9000){
                continue;
            }
            node1Runtime.getManagementView().addNode(getConnectionString(port), workflowNumRetry, timeout, pollPeriod);
            //node1Runtime.invalidateLayout();
            clusterSize++;
            assertThat(node1Runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSize);
        }

        for (int i = 0; i < scenarioRetry; i++) {
            System.out.println("Replay scenario");
            degradationScenario(servers, node1Runtime, table);
            healing(servers, node1Runtime, table);
        }

        for (CorfuServerDockerized server : servers.values()) {
            server.killContainer();
        }
    }

    private void degradationScenario(Map<Integer, CorfuServerDockerized> servers, CorfuRuntime node1Runtime, CorfuTable<String, String> table) {
        Set<Integer> excludedNodes = new HashSet<>();
        for (int i = 0; i < degradation; i++) {
            int port = Math.max(9001, 9000 + RND.nextInt(nodes));
            while(excludedNodes.contains(port)){
                port = Math.max(9001, 9000 + RND.nextInt(nodes));
            }
            excludedNodes.add(port);

            servers.get(port).killContainer();
            node1Runtime.getManagementView().removeNode(getConnectionString(port), workflowNumRetry, timeout, pollPeriod);
            servers.remove(port);

            table.put(String.valueOf(RND.nextLong()), String.valueOf(RND.nextLong()));

            assertThat(node1Runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(servers.size());
        }
    }

    private void healing(Map<Integer, CorfuServerDockerized> servers, CorfuRuntime node1Runtime, CorfuTable<String, String> table) throws Exception{
        ServerMode serverMode = ServerMode.CLUSTER;

        for (int i = 1; i < nodes; i++) {
            int port = 9000 + i;
            if(servers.containsKey(port)){
                continue;
            }

            servers.put(port, startServer(port, networkName, serverMode));
            FAKE_DNS.addForwardResolution("node" + port, InetAddress.getByName("127.0.0.1"));

            try {
                node1Runtime.getManagementView().addNode(getConnectionString(port), workflowNumRetry, timeout, pollPeriod);
                assertThat(node1Runtime.getLayoutView().getLayout().getAllServers().size()).isEqualTo(servers.size());
            } catch (Exception er){
                log.error("Can't add node: {}, so skip it", "node" + port, er);
            }
            table.put(String.valueOf(RND.nextLong()), String.valueOf(RND.nextLong()));
        }
    }

    private CorfuServerDockerized startServer(int port, String networkName, ServerMode serverMode) throws Exception {
        CorfuServerParams server1Params = CorfuServerParams.builder()
                .serverMode(serverMode)
                .port(port)
                .hostName("node" + port)
                .build();
        CorfuServerDockerized server = new CorfuServerDockerized(server1Params, docker, networkName);
        server.start();
        return server;
    }

    private void setupNetwork(String networkName) throws DockerException, InterruptedException {
        NetworkConfig networkConfig = NetworkConfig.builder()
                .checkDuplicate(true)
                .attachable(true)
                .name(networkName)
                .build();

        docker.removeNetwork(networkName);
        docker.createNetwork(networkConfig);
    }

    private String getConnectionString(int port) {
        return "node" + port + ":" + port;
    }
}

