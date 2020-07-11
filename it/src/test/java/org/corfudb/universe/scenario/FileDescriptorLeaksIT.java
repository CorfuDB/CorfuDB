package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.UniverseManager.UniverseWorkflow;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.spec.FileDescriptorLeaksSpec;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.Test;

import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class FileDescriptorLeaksIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after one node down.
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Stop one node
     * 3) Verify layout, cluster status and data path
     * 4) Recover cluster by restarting the stopped node
     * 5) Verify layout, cluster status and data path again
     */
    @Test(timeout = 300000)
    public void fileDescriptorLeaksTest() {

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
        CorfuServer server0 = corfuCluster.getFirstServer();

        FileDescriptorLeaksSpec.builder()
                .server(server0)
                .corfuClient(corfuClient)
                .build()
                .resetServer()
                .check();
    }
}
