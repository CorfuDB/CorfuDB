package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.logreplication.infrastructure.CorfuReplicationServer;
import org.corfudb.logreplication.infrastructure.CorfuReplicationSiteManagerAdapter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

public class CorfuReplicationE2EIT extends AbstractIT {
    static final int MAX_RETRY = 4;
    @Test
    public void testLogReplicationEndToEnd() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Process activeCorfu = null;
        Process standbyCorfu = null;

        try {
            final String streamA = "Table001";

            final int activeSiteCorfuPort = 9000;
            final int standbySiteCorfuPort = 9001;

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

            CorfuRuntime activeRuntime1 = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            activeRuntime1.parseConfigurationString(activeEndpoint);
            activeRuntime1.connect();

            //CorfuRuntime standbyRuntime = new CorfuRuntime(standbyEndpoint).setTransactionLogging(true).connect();
            CorfuRuntime standbyRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            standbyRuntime.parseConfigurationString(standbyEndpoint);
            standbyRuntime.connect();

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

            // Start Log Replication Server on Standby Site
            CorfuReplicationServer serverA = new CorfuReplicationServer(new String[]{"9010"});
            Thread siteAThread = new Thread(serverA);
            System.out.println("Start Corfu Log Replication Server on 9010");
            siteAThread.start();

            CorfuReplicationServer serverB = new CorfuReplicationServer(new String[]{"9020"});
            Thread siteBThread = new Thread(serverB);
            System.out.println("Start Corfu Log Replication Server on 9020");
            siteBThread.start();


            // Wait until data is fully replicated
            System.out.println("Wait ... Data is being replicated ...");
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
            System.out.println("Wait ... Data is being replicated ...");
            while (mapAStandby.size() != (numWrites + numWrites/2)) {
                //
            }

            // Verify data is present in Standby Site (delta)
            assertThat(mapAStandby.size()).isEqualTo(numWrites + numWrites/2);

            for (int i = 0; i < (numWrites + numWrites/2) ; i++) {
                assertThat(mapAStandby.containsKey(String.valueOf(i)));
            }
            assertThat(mapAStandby.keySet().containsAll(mapA.keySet()));

            String primary = serverA.getSiteManagerAdapter().getCrossSiteConfiguration().getPrimarySite().getSiteId();
            String currentPimary = serverA.getSiteManagerAdapter().getCrossSiteConfiguration().getPrimarySite().getSiteId();

            // Wait till site role change and new transfer done.
            assertThat(currentPimary).isEqualTo(primary);
            System.out.print("\ncurrent standbyTail " + standbyRuntime.getAddressSpaceView().getLogTail() + " activeTail " + activeRuntime.getAddressSpaceView().getLogTail());
            System.out.print("\nmapA1 keySet " + mapA.keySet() + " mapAstandby " + mapAStandby.keySet());
            System.out.print("\nwait for site switch " + activeRuntime.getAddressSpaceView().getLogTail());

            CorfuReplicationSiteManagerAdapter siteManagerAdapter = serverA.getSiteManagerAdapter();
            synchronized (siteManagerAdapter) {
                serverA.getSiteManagerAdapter().wait();
            }
            currentPimary = serverA.getSiteManagerAdapter().getCrossSiteConfiguration().getPrimarySite().getSiteId();
            assertThat(currentPimary).isNotEqualTo(primary);
            System.out.print("\nVerified Site Role Change " + activeRuntime.getAddressSpaceView().getLogTail());

            // Add new updates (deltas)
            for (int i=numWrites/2; i < numWrites; i++) {
                standbyRuntime.getObjectsView().TXBegin();
                mapAStandby.put(String.valueOf(numWrites + i), numWrites + i);
                standbyRuntime.getObjectsView().TXEnd();
            }

            // Verify data is present in Standby Site
            System.out.println("\nWait ... Data is being replicated ...");

            // Write to StreamA on Active Site
            CorfuTable<String, Integer> mapA1 = activeRuntime1.getObjectsView()
                    .build()
                    .setStreamName(streamA)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            // Will fix this part later
            int retry = 0;
            while (!mapA1.keySet().containsAll(mapAStandby.keySet()) && retry++ < MAX_RETRY) {
                System.out.print("\nstandbyTail " + standbyRuntime.getAddressSpaceView().getLogTail() + " activeTail " + activeRuntime.getAddressSpaceView().getLogTail());
                System.out.print("\nmapA1 keySet " + mapA1.keySet() + " mapAstandby " + mapAStandby.keySet());
            }

            for (int i = 0; i < 2*numWrites ; i++) {
                //assertThat(mapA.containsKey(String.valueOf(i)));
            }

        } finally {
            executorService.shutdownNow();

            if (activeCorfu != null) {
                activeCorfu.destroy();
            }

            if (standbyCorfu != null) {
                standbyCorfu.destroy();
            }
        }
    }
}
