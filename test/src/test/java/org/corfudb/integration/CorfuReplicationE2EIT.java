package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuReplicationE2EIT extends AbstractIT {

    public void testLogReplicationEndToEnd(boolean useNetty, boolean runProcess) throws Exception {
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

            final int numWrites = 4;

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
                activeReplicationServer = runReplicationServer(activeReplicationServerPort, useNetty);

                // Start Log Replication Server on Standby Site
                standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, useNetty);
            } else {
                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"--custom-transport", String.valueOf(activeReplicationServerPort)});
                });

                executorService.submit(() -> {
                    CorfuInterClusterReplicationServer.main(new String[]{"--custom-transport", String.valueOf(standbyReplicationServerPort)});
                });
            }

            // Wait until data is fully replicated
            System.out.println("\nWait ... Snapshot log replication in progress ...");
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
            System.out.println("\nWait ... Delta log replication in progress ...");
            while (mapAStandby.size() != (numWrites + numWrites/2)) {
            }

            // Verify data is present in Standby Site (delta)
            assertThat(mapAStandby.size()).isEqualTo(numWrites + numWrites/2);

            for (int i = 0; i < (numWrites + numWrites/2) ; i++) {
                assertThat(mapAStandby.containsKey(String.valueOf(i)));
            }
            System.out.print("\nTest succeeds");

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

    @Test
    public void testLogReplicationEndToEndWithNetty() throws Exception {
        testLogReplicationEndToEnd(true, false);
    }

    @Test
    public void testLogReplicationEndToEndWithCustom() throws Exception {
        testLogReplicationEndToEnd(false, false);
    }
}
