package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class CorfuReplicationE2EIT extends AbstractIT {

    private boolean useNetty;

    public CorfuReplicationE2EIT(boolean netty) {
        this.useNetty = netty;
    }

    // Static method that generates and returns test data (automatically test for two transport protocols: netty and GRPC)
    @Parameterized.Parameters
    public static Collection input() {
        return Arrays.asList(Boolean.FALSE, Boolean.TRUE);
    }

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

            // Start Log Replication Server on Active Site
             activeReplicationServer = runReplicationServer(activeReplicationServerPort, useNetty);
//            executorService.submit(() -> {
//                CorfuReplicationServer.main(new String[]{"--custom-transport", String.valueOf(activeReplicationServerPort)});
//            });

            // Start Log Replication Server on Standby Site
            standbyReplicationServer = runReplicationServer(standbyReplicationServerPort, useNetty);
//            executorService.submit(() -> {
//                CorfuReplicationServer.main(new String[]{"--custom-transport", String.valueOf(standbyReplicationServerPort)});
//            });

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
