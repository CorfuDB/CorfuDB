package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseParams;
import org.junit.Test;

import java.util.Optional;

@Slf4j
public class ProduceData extends GenericIntegrationTest {
    @Test(timeout = 9000000)
    public void produceData(){

        UniverseManager processUniverseManager = UniverseManager.builder()
                .testName("RANDOM_TEST")
                .universeMode(Universe.UniverseMode.PROCESS)
                .corfuServerVersion("0.3.0-SNAPSHOT")
                .build();

        processUniverseManager.workflow(wf -> {
            wf.setupProcess(fix -> {
                fix.getCluster().name("ST");
                fix.getFixtureUtilBuilder().initialPort(Optional.of(9000));
                fix.getUniverse().cleanUpEnabled(false);
            });

            wf.initUniverse();

            CorfuCluster corfuCluster = wf.getUniverse()
                    .getGroup(wf.getFixture().data().getGroupParamByIndex(0).getName());

            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            corfuClient.invalidateLayout();
            log.info("LIOT: {}", corfuClient.getLayout());

        });

    }
}
