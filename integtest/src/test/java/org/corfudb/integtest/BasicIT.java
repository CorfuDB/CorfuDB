package org.corfudb.integtest;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.NetworkConfig;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.*;

import static org.junit.Assert.assertEquals;

@Slf4j
public class BasicIT {
    private static final FakeDns FAKE_DNS = FakeDns.getInstance();
    private static final Random RND = new SecureRandom();

    private final DockerClient docker;

    private final String streamName = "stream";
    private final String networkName = "corfunetwork";

    public BasicIT() throws Exception {
        this.docker = DefaultDockerClient.fromEnv().build();
    }

    @BeforeClass
    public static void init() {
        FAKE_DNS.install();
    }

    @Test
    public void testTest() throws Exception {
        final int server1Port = 9000;
        final String localhost = "127.0.0.1";

        setupNetwork(networkName);

        CorfuServerDockerized server = startServer(server1Port, networkName, ServerMode.SINGLE);
        FAKE_DNS.addForwardResolution("node" + server1Port, InetAddress.getByName(localhost));

        CorfuRuntime node1Runtime = new CorfuRuntime(getConnectionString(9000)).setCacheDisabled(true).connect();

        CorfuTable<String, String> table = node1Runtime
                .getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(streamName)
                .open();

        String value = String.valueOf(RND.nextInt());
        table.put(value, value);

        assertEquals(table.get(value), value);

        server.killContainer();
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

        try {
            docker.removeNetwork(networkName);
        } catch (Exception ex){
            //ignore
        }
        docker.createNetwork(networkConfig);
    }

    private String getConnectionString(int port) {
        return "node" + port + ":" + port;
    }
}

