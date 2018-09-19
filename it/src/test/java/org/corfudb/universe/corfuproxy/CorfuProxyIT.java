package org.corfudb.universe.corfuproxy;

import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuProxyIT {
    private static final String[] servers = {"localhost:9001", "localhost:9002"};
    private static final List<Process> processes = new ArrayList<>();

    @BeforeClass
    public static void startServers() throws Exception {
        final File directory = new File("").getAbsoluteFile().getParentFile();

        // Start corfu servers
        for (String server : servers) {
            String port = server.split(":")[1];
            String[] command = {"bin/corfu_server", "-ms", port};
            processes.add(runServer(command, directory));
        }

        // Start proxy server
        String[] proxyCmd = {"java", "-cp", "target/*", CorfuProxyServer.class.getCanonicalName()};
        processes.add(runServer(proxyCmd, new File("").getAbsoluteFile()));

        Thread.sleep(5000);
    }

    @AfterClass
    public static void killServers() {
        processes.forEach(Process::destroyForcibly);
    }

    private static Process runServer(String[] command, File directory) throws Exception {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(command);
        builder.directory(directory);

        return builder.start();
    }

    @Test(timeout = 60000)
    public void ClientOperationTest() throws InterruptedException {
        CorfuProxyClient client = new CorfuProxyClient("localhost", 7001).setup();
        Semaphore semaphore = new Semaphore(1);

        semaphore.acquire();

        client.addLayoutServer(
                servers[0],
                resp -> {
                    assertThat(resp.hasMessageId()).isEqualTo(true);
                    assertThat(resp.hasErrorMsg()).isEqualTo(false);
                    semaphore.release();
                });

        semaphore.acquire();

        client.connect(resp -> {
            assertThat(resp.hasMessageId()).isEqualTo(true);
            assertThat(resp.hasErrorMsg()).isEqualTo(false);
            semaphore.release();
        });

        semaphore.acquire();

        client.addNode(servers[1], 1, Duration.ofSeconds(1), Duration.ofMillis(50), resp -> {
            assertThat(resp.hasMessageId()).isEqualTo(true);
            assertThat(resp.hasErrorMsg()).isEqualTo(false);
            semaphore.release();
        });

        semaphore.acquire();

        client.getAllServers(resp -> {
            assertThat(resp.hasMessageId()).isEqualTo(true);
            assertThat(resp.hasErrorMsg()).isEqualTo(false);

            Set<String> serversSet = Sets.newHashSet(servers);
            Set<String> serversRespSet = Sets.newHashSet(resp.getGetAllServersResponse().getServersList());
            assertThat(serversSet.size()).isEqualTo(serversRespSet.size());
            assertThat(serversSet.containsAll(serversRespSet)).isEqualTo(true);
            semaphore.release();
        });

        semaphore.acquire();

        client.removeNode(servers[1], 3, Duration.ofMinutes(5), Duration.ofMillis(50), resp -> {
            assertThat(resp.hasMessageId()).isEqualTo(true);
            assertThat(resp.hasErrorMsg()).isEqualTo(false);
            semaphore.release();
        });

        semaphore.acquire();

        client.invalidateLayout(resp -> {
            assertThat(resp.hasMessageId()).isEqualTo(true);
            assertThat(resp.hasErrorMsg()).isEqualTo(false);
            semaphore.release();
        });

        semaphore.acquire();

        client.getAllServers(resp -> {
            assertThat(resp.hasMessageId()).isEqualTo(true);
            assertThat(resp.hasErrorMsg()).isEqualTo(false);

            Set<String> serversSet = Sets.newHashSet(servers);
            Set<String> serversRespSet = Sets.newHashSet(resp.getGetAllServersResponse().getServersList());
            serversSet.remove(servers[1]);
            assertThat(serversSet.size()).isEqualTo(serversRespSet.size());
            assertThat(serversSet.containsAll(serversRespSet)).isEqualTo(true);
            semaphore.release();
        });

        semaphore.acquire();
    }
}
