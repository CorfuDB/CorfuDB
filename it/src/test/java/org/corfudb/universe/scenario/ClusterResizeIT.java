package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.Test;
import org.slf4j.event.Level;

@Slf4j
public class ClusterResizeIT extends GenericIntegrationTest {

    /**
     * Test cluster behavior after add/remove nodes
     * <p>
     * 1) Deploy and bootstrap a three nodes cluster
     * 2) Sequentially remove two nodes from cluster
     * 3) Verify layout and data path
     * 4) Sequentially add two nodes back into cluster
     * 5) Verify layout and data path again
     */
    @Test(timeout = 30000000)
    public void clusterResizeTest() {
        workflow(wf -> {
            wf.setupDocker(fixture -> {
                fixture.getCluster().numNodes(3);
                fixture.getLogging().enabled(true);
                fixture.getServer().logLevel(Level.TRACE);
            });

            wf.deploy();
            UniverseParams params = wf.getFixture().data();

            CorfuCluster corfuCluster = wf.getUniverse()
                    .getGroup(params.getGroupParamByIndex(0).getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();

            System.out.println("New iteration...");

            corfuClient.getRuntime().invalidateLayout();
            Layout layout = new Layout(corfuClient.getRuntime().getLayoutView().getLayout());
            log.info("Current layout: {}", layout);
            log.info("Sealing the next epoch...");
            corfuClient.getRuntime().getLayoutManagementView().sealEpoch(layout);
            log.info("... done sealing the epoch.");

            for (int i = 0; i < 100; i++) {
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            corfuClient.shutdown();
        });
    }
}
