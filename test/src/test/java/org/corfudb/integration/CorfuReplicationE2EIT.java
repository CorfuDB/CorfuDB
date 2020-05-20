package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.logreplication.infrastructure.CorfuReplicationServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

public class CorfuReplicationE2EIT extends AbstractIT {
    static final int MAX_RETRY = 20;
    static final long sleepTime = 2000;
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

            final int numWrites = 1000;

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
            System.out.println("Wait ... Snapshot Data is being replicated ...");
            while (mapAStandby.size() < numWrites) {
                sleep(sleepTime);
                System.out.print("mapAstandbysize " + mapAStandby.size() + " mapAsize " + mapA.size());
            }

            System.out.print("mapAstandbysize " + mapAStandby.size() + " mapAsize " + mapA.size());


            // Verify data is present in Standby Site
            assertThat(mapAStandby.size()).isEqualTo(numWrites);

            // Add new updates (deltas)
            for (int i=0; i < numWrites/2; i++) {
                activeRuntime.getObjectsView().TXBegin();
                mapA.put(String.valueOf(numWrites + i), numWrites + i);
                activeRuntime.getObjectsView().TXEnd();
            }

            // Verify data is present in Standby Site
            System.out.println("Wait ... Delta Data is being replicated ...");
            while (mapAStandby.size() < (numWrites + numWrites/2)) {
                sleep(sleepTime);
                System.out.print("mapAstandbysize " + mapAStandby.size() + " mapAsize " + mapA.size());
            }
            System.out.print("mapAstandbysize " + mapAStandby.size() + " mapAsize " + mapA.size());

            // Verify data is present in Standby Site (delta)
            assertThat(mapAStandby.size()).isEqualTo(numWrites + numWrites/2);

            for (int i = 0; i < (numWrites + numWrites/2) ; i++) {
                assertThat(mapAStandby.containsKey(String.valueOf(i)));
            }
            assertThat(mapAStandby.keySet().containsAll(mapA.keySet()));

            String primary = serverA.getSiteManagerAdapter().getSiteConfig().getPrimarySite().getSiteId();
            String currentPimary = serverA.getSiteManagerAdapter().getSiteConfig().getPrimarySite().getSiteId();

            return;

            /*
            // Wait till site role change and new transfer done.
            assertThat(currentPimary).isEqualTo(primary);
            System.out.print("\ncurrent mapAstandby tail " + standbyRuntime.getAddressSpaceView().getLogTail() + " mapA tail " + activeRuntime.getAddressSpaceView().getLogTail());
            System.out.print("\nmapA1 keySet " + mapA.keySet() + " mapAstandby " + mapAStandby.keySet());
            System.out.print("\nwait for site switch " + activeRuntime.getAddressSpaceView().getLogTail());

            CorfuReplicationDiscoveryService discoveryService = serverA.getReplicationDiscoveryService();
            synchronized (discoveryService) {
                discoveryService.wait();
            }

            currentPimary = serverA.getSiteManagerAdapter().getSiteConfig().getPrimarySite().getSiteId();
            assertThat(currentPimary).isNotEqualTo(primary);
            System.out.print("\nVerified Site Role Change " + activeRuntime.getAddressSpaceView().getLogTail());

            // Write to StreamA on Active Site
            CorfuTable<String, Integer> mapA1 = activeRuntime1.getObjectsView()
                    .build()
                    .setStreamName(streamA)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            System.out.print("\nstandbyTail " + standbyRuntime.getAddressSpaceView().getLogTail() + " activeTail " + activeRuntime.getAddressSpaceView().getLogTail());
            sleep(sleepTime);

            System.out.print("\nwriting to the new active new data");
            // Add new updates (deltas)
            for (int i=numWrites/2; i < numWrites; i++) {
                standbyRuntime.getObjectsView().TXBegin();
                mapAStandby.put(String.valueOf(numWrites + i), numWrites + i);
                standbyRuntime.getObjectsView().TXEnd();
            }

            // Verify data is present in Standby Site
            System.out.println("\nWait ... Data is being replicated ...");

            System.out.print("\nmapAstandby tail " + standbyRuntime.getAddressSpaceView().getLogTail() + " mapA tail " + activeRuntime.getAddressSpaceView().getLogTail());
            System.out.print("\nmapA1 keySet " + mapA1.keySet() + " mapAstandby " + mapAStandby.keySet());

            int retry = 0;
            while (mapA1.keySet().size() != mapAStandby.keySet().size() && retry++ < MAX_RETRY) {
                System.out.print("\nafter writing keySize " + mapA1.keySet() + " real active " + mapAStandby.keySet());
                System.out.print("\nstandbyTail " + standbyRuntime.getAddressSpaceView().getLogTail() + " activeTail " + activeRuntime.getAddressSpaceView().getLogTail());
                sleep(sleepTime);
            }

            System.out.print("\nmapA1 keySet " + mapA1.keySet() + " mapAstandby " + mapAStandby.keySet());

            for (int i = 0; i < 2*numWrites ; i++) {
                assertThat(mapA1.containsKey(String.valueOf(i))).isTrue();
            }

            System.out.print("\nmapA1 keySet " + mapA1.keySet() + " mapAstandby " + mapAStandby.keySet());
            System.out.print("\nmapA keySet " + mapA.keySet() + " mapAstandby " + mapAStandby.keySet());
*/
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
