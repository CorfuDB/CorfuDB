package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.docker.DockerCorfuServer;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.universe.scenario.ScenarioUtils.waitForUnresponsiveServersChange;

// 3 Nodes are brought up, 1 node has all the data, let transfer finish, put one node in unresponsive list
// Transfer size: None, 100Mb
// Introduce Latencies: 500ms every 5 seconds, 1000ms every 5 seconds
// Network stretcher latencies: Default values, 5times faster
// Measure: Number of layout reconfigurations, Failure Detection Times

@Slf4j
public class NetworkStretcherTest extends GenericIntegrationTest {

    private static final int DELAY_MILLIS = 5000; // 100, 500, 1000
    private static final Duration DURATION_OF_DELAYS = Duration.ofSeconds(10);
    private static final Duration DURATION_BETWEEN_DELAYS = Duration.ofSeconds(30); // 15, 10, 5
    private static final Duration TEST_DURATION = Duration.ofMinutes(5);

    private CompletableFuture<Void> introduceIntermittentDelay(DockerCorfuServer dockerCorfuServer, AtomicBoolean keepRunning) {
        return CompletableFuture.supplyAsync(() -> {
            while (keepRunning.get()) {
                log.info("Introducing delay");
                dockerCorfuServer.introduceDelay(DELAY_MILLIS);
                Sleep.sleepUninterruptibly(DURATION_OF_DELAYS);
                log.info("Removing delay");
                dockerCorfuServer.removeDelay();
                Sleep.sleepUninterruptibly(DURATION_BETWEEN_DELAYS);
            }
            return null;
        });
    }

    @Test(timeout = 30000000)
    public void test() {
        workflow(wf -> {
            wf.setupDocker(fix -> fix.getLogging().enabled(true));
            wf.deploy();
            CorfuCluster corfuCluster = wf.getUniverse()
                    .getGroup(wf.getFixture().data().getGroupParamByIndex(0).getName());
            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            DockerCorfuServer server1 = (DockerCorfuServer) corfuCluster.getServerByIndex(1);
            server1.pause();
            waitForUnresponsiveServersChange(size -> size == 1, corfuClient);
        });
    }
}
