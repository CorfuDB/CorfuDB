package org.corfudb.universe.scenario;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.UniverseManager.UniverseWorkflow;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.scenario.fixture.Fixture;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class WriteAfterResetIT extends GenericIntegrationTest {

    /**
     * Test a log unit behavior after the reset.
     * <p>
     * 1) Deploy a single node cluster.
     * 2) Connect a client's runtime to the node.
     * 3) Write 100 addresses to the log unit.
     * 4) Perform a reset.
     * 5) Try writing record to the log unit, the write should not go through the router,
     *    as the server is not bootstrapped.
     * 6) Bootstrap a node with a new layout.
     * 7) Write a record to the log unit.
     * 8) The write should go through, as the server is now bootstrapped.
     */

    private LogData getLogData(long address) {
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogData ld = new LogData(DataType.DATA, b);
        ld.setGlobalAddress(address);
        ld.setEpoch(0L);
        return ld;
    }

    @Test(timeout = 300000)
    public void writeAfterResetTest() {
        workflow(wf -> {

                    wf.setupDocker(fixture -> {
                        fixture.getCluster().numNodes(1);
                    });

                    wf.deploy();
                    try {
                        writeAfterReset(wf);
                    } catch (Exception e) {
                        Assertions.fail("Test failed: " + e);
                    }

                }
        );
    }

    private void writeAfterReset(UniverseWorkflow<Fixture<UniverseParams>> wf) throws Exception {
        UniverseParams params = wf.getFixture().data();
        CorfuCluster<CorfuServer, CorfuClusterParams> corfuCluster = wf.getUniverse()
                .getGroup(params.getGroupParamByIndex(0).getName());
        CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
        corfuClient.getRuntime().invalidateLayout();
        Layout layout = corfuClient.getLayout();
        assertThat(layout.getFirstSegment().getAllLogServers().size()).isEqualTo(1);

        // Start writing to some stream.
        CorfuTable<String, String> table =
                corfuClient.createDefaultCorfuTable(Fixtures.TestFixtureConst.DEFAULT_STREAM_NAME);
        for (int i = 0; i < Fixtures.TestFixtureConst.DEFAULT_TABLE_ITER; i++) {
            table.put(String.valueOf(i), String.valueOf(i));
        }

        String endpoint = corfuCluster.getFirstServer().getEndpoint();
        CorfuClient resetClient = corfuCluster.getLocalCorfuClient();
        Layout previousLayout = corfuClient.getLayout();

        boolean resetHappened = resetClient.getRuntime()
                .getLayoutView()
                .getRuntimeLayout()
                .getBaseClient(endpoint)
                .reset()
                .join();

        if (resetHappened) {
            Duration resetSleepDuration = Duration.ofMillis(2000);
            long address = 100L;
            TimeUnit.MILLISECONDS.sleep(resetSleepDuration.toMillis());
            // After the reset write some data at address 100.
            corfuClient.getRuntime().getLayoutView()
                    .getRuntimeLayout(previousLayout)
                    .getLogUnitClient(endpoint)
                    .write(getLogData(address)).whenComplete((value, ex) -> {
                if (ex != null) {
                    // NotSealedException should be thrown.
                    assertThat(ex).hasCauseExactlyInstanceOf(NoBootstrapException.class);

                } else {
                    Assertions.fail("NotSealedException should have been thrown.");
                }
            }).exceptionally(ex -> {
                // Bootstrap a new 1 node cluster.
                corfuCluster.bootstrap();
                CorfuClient corfuClient2 = corfuCluster.getLocalCorfuClient();
                corfuClient2.invalidateLayout();
                // Write at address 100.
                boolean writeWentThrough = corfuClient2.getRuntime().getLayoutView()
                        .getRuntimeLayout()
                        .getLogUnitClient(endpoint)
                        .write(getLogData(address)).join();
                if(writeWentThrough){
                    // Read at address 100.
                    ReadResponse response = corfuClient2.getRuntime().getLayoutView()
                            .getRuntimeLayout()
                            .getLogUnitClient(endpoint)
                            .read(address)
                            .join();
                    assertThat(new String(response.getAddresses().get(address).getData())).contains("Payload");
                }
                else{
                    Assertions.fail("Write after bootstrap should succeeded.");
                }
                return true;
            }).join();


        } else {
            Assertions.fail("Reset did not happen.");
        }
    }

}
