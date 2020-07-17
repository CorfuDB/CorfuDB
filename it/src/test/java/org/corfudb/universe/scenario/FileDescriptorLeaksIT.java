package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.UniverseManager.UniverseWorkflow;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.spec.FileDescriptorLeaksSpec;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class FileDescriptorLeaksIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after one node down.
     * <p>
     * 1) Deploy and bootstrap a one node cluster
     * 2) Restart the node
     * 3) Check if there are any resource leaks
     */
    @Test(timeout = 30000000)
    public void fileDescriptorLeaksBaseServerResetTest() {

        workflow(wf -> {
            wf.setupDocker(fixture -> {
                fixture.getCluster().numNodes(1);
            });
            wf.deploy();

            try {
                resourceLeaks(wf);
            } catch (Exception e) {
                fail("Test failed", e);
            }
        });
    }

    private void resourceLeaks(UniverseWorkflow<Fixture<UniverseParams>> wf) throws Exception {
        String groupName = wf.getFixture().data().getGroupParamByIndex(0).getName();
        CorfuCluster corfuCluster = wf.getUniverse().getGroup(groupName);

        CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

        CorfuTable<String, String> table = corfuClient
                .createDefaultCorfuTable(DEFAULT_STREAM_NAME);

        for (int i = 0; i < 100; i++) {
            table.put(String.valueOf(i), String.valueOf(i));
        }

        //Should stop one node and then restart
        CorfuServer server = corfuCluster.getFirstServer();

        FileDescriptorLeaksSpec.builder()
                .server(server)
                .corfuClient(corfuClient)
                .build()
                .resetServer()
                .timeout()
                .check();
    }

    /**
     * LogUnitServer - file descriptors leak detector
     */
    @Test(timeout = 300000)
    @Ignore
    public void fileDescriptorLeaksLogUnitServerResetTest() {

        workflow(wf -> {
            wf.setupDocker(fixture -> {
                fixture.getCluster().numNodes(1);
            });
            wf.deploy();

            try {
                resourceLeaksLogUnit(wf);
            } catch (Exception e) {
                fail("Test failed", e);
            }
        });
    }

    private void resourceLeaksLogUnit(UniverseWorkflow<Fixture<UniverseParams>> wf) throws Exception {
        String groupName = wf.getFixture().data().getGroupParamByIndex(0).getName();
        CorfuCluster corfuCluster = wf.getUniverse().getGroup(groupName);

        CorfuServer server = corfuCluster.getFirstServer();
        CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

        final CorfuTable<String, String> table = corfuClient
                .createDefaultCorfuTable(DEFAULT_STREAM_NAME);

        CompletableFuture<Void> writes1 = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 1_000_000; i += 2) {
                if (i % 1000 == 0) {
                    System.out.println((i / 2) + ", of even addresses has written");
                }
                String data = RandomStringUtils.randomAlphabetic(128);
                table.put(String.valueOf(i), data);
            }
        });

        CompletableFuture<Void> writes2 = CompletableFuture.runAsync(() -> {
            for (int i = 1; i < 1_000_000; i += 2) {
                if (i % 1001 == 0) {
                    System.out.println((i / 2) + ", of odd addresses has written");
                }
                String data = RandomStringUtils.randomAlphabetic(128);
                table.put(String.valueOf(i), data);
            }
        });

        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 1_000; i++) {
                log.info("Iteration: " + i);
                try {
                    FileDescriptorLeaksSpec.builder()
                            .server(server)
                            .corfuClient(corfuClient)
                            .build()
                            .resetLogUnitServer(1)
                            .timeout(Duration.ofSeconds(180))
                            .check();
                } catch (InterruptedException e) {
                    fail("Interrupted");
                }
            }
        }).join();

        writes1.completeExceptionally(new RuntimeException("completed"));
        writes2.completeExceptionally(new RuntimeException("completed"));
    }
}
