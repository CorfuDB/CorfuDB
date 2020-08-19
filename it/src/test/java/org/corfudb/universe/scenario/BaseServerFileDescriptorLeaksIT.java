package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.UniverseManager.UniverseWorkflow;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.spec.FileDescriptorLeaksSpec;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.Ignore;
import org.junit.Test;

import static org.corfudb.universe.scenario.fixture.Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class BaseServerFileDescriptorLeaksIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after one node down.
     * <p>
     * 1) Deploy and bootstrap a one node cluster
     * 2) Restart the node
     * 3) Check if there are any resource leaks
     */
    @Test(timeout = 300_000)
    @Ignore
    public void fileDescriptorLeaksBaseServerResetTest() {

        workflow(wf -> {
            wf.setupDocker(fixture -> fixture.getCluster().numNodes(1));
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
        CorfuCluster<Node, GroupParams<NodeParams>> corfuCluster = wf.getUniverse().getGroup(groupName);

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
}