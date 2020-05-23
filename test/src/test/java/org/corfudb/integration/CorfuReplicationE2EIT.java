package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.logreplication.infrastructure.CorfuReplicationServer;
import org.corfudb.logreplication.infrastructure.DefaultSiteManager;
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
    static final int MAX_RETRY = 20;
    static final long sleepTime = 1000;

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

            final int numWrites = 2000;

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

            // Wait until data is fully replicated
            System.out.print("\nWait ... Snapshot Data is being replicated ...");
            while (mapAStandby.size() < numWrites) {
                System.out.print("\nmapAstandby size " + mapAStandby.size() + " mapAsize " + mapA.size());
                sleep(sleepTime);
            }

            System.out.print("\nmapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            // Verify data is present in Standby Site
            assertThat(mapAStandby.size()).isEqualTo(numWrites);

            sleep(sleepTime);
            // Add new updates (deltas)
            for (int i=0; i < numWrites/2; i++) {
                activeRuntime.getObjectsView().TXBegin();
                mapA.put(String.valueOf(numWrites + i), numWrites + i);
                activeRuntime.getObjectsView().TXEnd();
            }

            // Verify data is present in Standby Site
            System.out.print("\nWait ... Delta Data is being replicated ...");
            while (mapAStandby.size() < (numWrites + numWrites/2)) {
                sleep(sleepTime);
                System.out.print("\nmapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                        " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());
            }
            System.out.print("\nmapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapAsize " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            // Verify data is present in Standby Site (delta)
            assertThat(mapAStandby.size()).isEqualTo(numWrites + numWrites/2);

            for (int i = 0; i < (numWrites + numWrites/2) ; i++) {
                assertThat(mapAStandby.containsKey(String.valueOf(i)));
            }

            assertThat(mapAStandby.keySet().containsAll(mapA.keySet()));

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

    //@Test
    public void testLogReplicationEnd2EndWithSiteSwitch() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Process activeCorfu = null;
        Process standbyCorfu = null;
        try {
            final String streamA = "Table001";

            final int activeSiteCorfuPort = 9000;
            final int standbySiteCorfuPort = 9001;

            final String activeEndpoint = DEFAULT_HOST + ":" + activeSiteCorfuPort;
            final String standbyEndpoint = DEFAULT_HOST + ":" + standbySiteCorfuPort;

            final int numWrites = 500;

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
            System.out.print("\nStart Corfu Log Replication Server on 9010");
            siteAThread.start();

            CorfuReplicationServer serverB = new CorfuReplicationServer(new String[]{"9020"});
            Thread siteBThread = new Thread(serverB);
            System.out.println("Start Corfu Log Replication Server on 9020");
            siteBThread.start();


            // Wait until data is fully replicated
            System.out.print("\nWait ... Snapshot Data is being replicated ...");
            while (mapAStandby.size() < numWrites) {
                System.out.print("\nmapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                        " mapAsize " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());
                sleep(sleepTime);
            }

            System.out.print("\nmapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            // Verify data is present in Standby Site
            assertThat(mapAStandby.size()).isEqualTo(numWrites);

            // Add new updates (deltas)
            for (int i=0; i < numWrites/2; i++) {
                activeRuntime.getObjectsView().TXBegin();
                mapA.put(String.valueOf(numWrites + i), numWrites + i);
                activeRuntime.getObjectsView().TXEnd();
            }

            // Verify data is present in Standby Site
            System.out.println("\nWait ... Delta Data is being replicated ...");
            while (mapAStandby.size() < mapA.size()) {
                sleep(sleepTime);
                System.out.print("\nmapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                        " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());
            }

            System.out.print("\nmapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            // Verify data is present in Standby Site (delta)
            assertThat(mapAStandby.size()).isEqualTo(numWrites + numWrites/2);

            for (int i = 0; i < (numWrites + numWrites/2) ; i++) {
                assertThat(mapAStandby.containsKey(String.valueOf(i)));
            }

          assertThat(mapAStandby.keySet().containsAll(mapA.keySet()));

            String primary = serverA.getSiteManagerAdapter().getSiteConfig().getPrimarySite().getSiteId();
            String currentPimary = serverA.getSiteManagerAdapter().getSiteConfig().getPrimarySite().getSiteId();


            // Wait till site role change and new transfer done.
            assertThat(currentPimary).isEqualTo(primary);
            System.out.print("\nbefore site switch mapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            DefaultSiteManager siteManager = (DefaultSiteManager) serverA.getSiteManagerAdapter();
            siteManager.getSiteManagerCallback().siteFlip = true;
            siteManager = (DefaultSiteManager) serverB.getSiteManagerAdapter();
            siteManager.getSiteManagerCallback().siteFlip = true;

            CorfuReplicationDiscoveryService discoveryService = serverA.getReplicationDiscoveryService();
            synchronized (discoveryService) {
                discoveryService.wait();
            }

            currentPimary = serverA.getSiteManagerAdapter().getSiteConfig().getPrimarySite().getSiteId();
            assertThat(currentPimary).isNotEqualTo(primary);
            System.out.print("\nVerified Site Role Change primary " + currentPimary);
            System.out.print("\nmapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            // Write to StreamA on Active Site
            CorfuTable<String, Integer> mapA1 = activeRuntime1.getObjectsView()
                    .build()
                    .setStreamName(streamA)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            sleep(sleepTime);
            System.out.print("\nSnapshot done: mapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            System.out.print("\nwriting to the new active new data");
            // Add new updates (deltas)
            for (int i=numWrites/2; i < numWrites; i++) {
                standbyRuntime.getObjectsView().TXBegin();
                mapAStandby.put(String.valueOf(numWrites + i), numWrites + i);
                standbyRuntime.getObjectsView().TXEnd();
            }

            // Verify data is present in Standby Site
            System.out.println("\nWait ... Data is being replicated ...");
            System.out.print("\nmapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            int retry = 0;
            while (mapA1.keySet().size() != mapAStandby.keySet().size()) {
                sleep(sleepTime);
                System.out.print("\nmapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                        " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());
            }

            System.out.print("\nmapA1 keySet " + mapA1.keySet().size() + " mapAstandby " + mapAStandby.keySet().size());

            for (int i = 0; i < 2*numWrites ; i++) {
                assertThat(mapA1.containsKey(String.valueOf(i))).isTrue();
            }

            System.out.print("\nmapAstandby size " + mapAStandby.size()  + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapAsize " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

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
