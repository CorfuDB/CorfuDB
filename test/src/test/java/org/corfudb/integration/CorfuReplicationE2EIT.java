package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class CorfuReplicationE2EIT extends AbstractIT {

    private String pluginConfigFilePath;
    private static boolean runProcess = true;

    public CorfuReplicationE2EIT(String plugin) {
        this.pluginConfigFilePath = plugin;
    }

    // Static method that generates and returns test data (automatically test for two transport protocols: netty and GRPC)
    @Parameterized.Parameters
    public static Collection input() {

        List<String> transportPlugins = Arrays.asList("src/test/resources/transport/grpcConfig.properties",
                "src/test/resources/transport/nettyConfig.properties");

        if(runProcess) {
            List<String> absolutePathPlugins = new ArrayList<>();
            transportPlugins.forEach(plugin -> {
                File f = new File(plugin);
                absolutePathPlugins.add(f.getAbsolutePath());
            });

            return absolutePathPlugins;
        }

        return transportPlugins;
    }

    /**
     * Test Log Replication End to End for snapshot and log entry sync. These tests emulate two sites,
     * one active and one standby. The active site is represented by one Corfu Database and one LogReplication Server,
     * and the standby the same. Data is written into the active datastore and log replication is initiated to test
     * snapshot sync and afterwards incremental updates are written to evaluate log entry sync.
     *
     * The transport (communication) layer is based on a plugin architecture. We have two sample plugins:
     * - GRPC
     * - Netty
     *
     * This is a parameterized test and both plugins are tested.
     *
     * @throws Exception
     */
    @Test
    public void testLogReplicationEndToEnd() throws Exception {

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Process activeCorfu = null;
        Process standbyCorfu = null;

        Process activeReplicationServer = null;
        Process standbyReplicationServer = null;

        try {
            final String streamA = "Table001";

            final int activeSiteCorfuPort = 9000;
            final int standbySiteCorfuPort = 9001;

            final int activeReplicationServerPort = 9010;
            final int standbyReplicationServerPort = 9020;

            final String activeEndpoint = DEFAULT_HOST + ":" + activeSiteCorfuPort;
            final String standbyEndpoint = DEFAULT_HOST + ":" + standbySiteCorfuPort;

            final int numWrites = 10;

            // Start Single Corfu Node Cluster on Active Site
            activeCorfu = runServer(activeSiteCorfuPort, true);

            // Start Corfu Cluster on Standby Site
            standbyCorfu = runServer(standbySiteCorfuPort, true);

            CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .build();

            CorfuRuntime activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            activeRuntime.parseConfigurationString(activeEndpoint);
            activeRuntime.connect();

            CorfuRuntime standbyRuntime = new CorfuRuntime(standbyEndpoint).connect();

            // Write to StreamA on Active Site
            CorfuTable<String, Integer> mapA = activeRuntime.getObjectsView()
                    .build()
                    .setStreamName(streamA)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            CorfuTable<String, Integer> mapAStandby = standbyRuntime.getObjectsView()
                    .build()
                    .setStreamName(streamA)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            assertThat(mapA.size()).isEqualTo(0);

            for (int i = 0; i < numWrites; i++) {
                activeRuntime.getObjectsView().TXBegin();
                mapA.put(String.valueOf(i), i);
                activeRuntime.getObjectsView().TXEnd();
            }

            assertThat(mapA.size()).isEqualTo(numWrites);

            // Confirm data does not exist on Standby Site
            assertThat(mapAStandby.size()).isEqualTo(0);

            if (runProcess) {
                // Start Log Replication Server on Active Site
                activeReplicationServer = runReplicationServer(activeReplicationServerPort, pluginConfigFilePath);

                // Start Log Replication Server on Standby Site
                standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, pluginConfigFilePath);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"test", "--plugin=" + pluginConfigFilePath, "--address=" + "localhost", String.valueOf(activeReplicationServerPort)});
                });

                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"test",  "--plugin=" + pluginConfigFilePath, "--address=" + "localhost", String.valueOf(standbyReplicationServerPort)});
                });
            }

            System.out.println("\nUsing transport defined in :: " + pluginConfigFilePath);

            // Wait until data is fully replicated
            System.out.println("Wait ... Snapshot log replication in progress ...");
            while (mapAStandby.size() != numWrites) {
                //
            }

            // Verify data is present in Standby Site
            assertThat(mapAStandby.size()).isEqualTo(numWrites);

            // Add new updates (deltas)
            for (int i=0; i < numWrites/2; i++) {
                activeRuntime.getObjectsView().TXBegin();
                mapA.put(String.valueOf(numWrites + i), numWrites + i);
                activeRuntime.getObjectsView().TXEnd();
            }

            // Verify data is present in Standby Site
            System.out.println("Wait ... Delta log replication in progress ...");
            while (mapAStandby.size() != (numWrites + numWrites/2)) {
                //
            }

            // Verify data is present in Standby Site (delta)
            assertThat(mapAStandby.size()).isEqualTo(numWrites + numWrites/2);

            for (int i = 0; i < (numWrites + numWrites/2) ; i++) {
                assertThat(mapAStandby.containsKey(String.valueOf(i)));
            }

            System.out.print("Test succeeds");

        } finally {

            executorService.shutdownNow();

            if (activeCorfu != null) {
                activeCorfu.destroy();
            }

            if (standbyCorfu != null) {
                standbyCorfu.destroy();
            }

            if (activeReplicationServer != null) {
                activeReplicationServer.destroy();
            }

            if (standbyReplicationServer != null) {
                standbyReplicationServer.destroy();
            }
        }
    }
}
