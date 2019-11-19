package org.corfudb.universe.scenario;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.UniverseManager;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.universe.Universe;
import org.junit.Test;

import java.util.Optional;

import static org.corfudb.universe.scenario.ScenarioUtils.waitForLayoutChange;

@Slf4j
public class ProduceData extends GenericIntegrationTest {
    @Test(timeout = 9000000)
    public void produceData() {

        UniverseManager processUniverseManager = UniverseManager.builder()
                .testName("RANDOM_TEST")
                .universeMode(Universe.UniverseMode.PROCESS)
                .corfuServerVersion("0.3.0-SNAPSHOT")
                .build();

        processUniverseManager.workflow(wf -> {
            wf.setupProcess(fix -> {
                fix.getLogging().enabled(true);
                fix.getCluster().name("ST");
                fix.getFixtureUtilBuilder().initialPort(Optional.of(9000));
                fix.getUniverse().cleanUpEnabled(false);
            });

            wf.initUniverse();

            CorfuCluster corfuCluster = wf.getUniverse()
                    .getGroup(wf.getFixture().data().getGroupParamByIndex(0).getName());


            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
            corfuClient.invalidateLayout();

            CorfuTable<String, String> table =
                    corfuClient.createDefaultCorfuTable(Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME);

            String endpoint = corfuCluster.getFirstServer().getEndpoint();
            log.info("{} disconnecting", endpoint);

            corfuCluster.getFirstServer().disconnect();

            log.info("Wait until {} is in unresponsive list", endpoint);

            waitForLayoutChange(layout -> layout.getUnresponsiveServers().contains(endpoint), corfuClient);

            log.info("Writing some data");

            for (int i = 0; i < Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
                table.put(String.valueOf(i), String.valueOf(i));
            }

            log.info("{} reconnecting", endpoint);

            corfuCluster.getFirstServer().reconnect();


            log.info("Wait until S/T restored");

            waitForLayoutChange(layout -> layout.getSegments().size() == 1, corfuClient);

        });

    }
}
