package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.infrastructure.logreplication.cluster.TopologyDescriptor;
import org.corfudb.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.logreplication.infrastructure.DefaultClusterManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.integration.AbstractIT.DEFAULT_HOST;
import static org.corfudb.integration.AbstractIT.runReplicationServer;
import static org.corfudb.integration.AbstractIT.runServer;

public class CorfuReplicationSiteConfigTestCaseTwoIT {
    final static int MAX_RETRY = 10;
    final static long sleepInterval = 10000;

    CorfuInterClusterReplicationServer serverA;
    CorfuInterClusterReplicationServer serverB;
    CorfuTable<String, Integer> mapA;
    CorfuTable<String, Integer> mapAStandby;
    CorfuRuntime activeRuntime;
    CorfuRuntime standbyRuntime;
    CorfuRuntime activeRuntime1;

    final String streamA = "Table001";

    final int activeSiteCorfuPort = 9000;
    final int standbySiteCorfuPort = 9001;

    final int activeReplicationServerPort = 9010;
    final int standbyReplicationServerPort = 9020;

    final String activeEndpoint = DEFAULT_HOST + ":" + activeSiteCorfuPort;
    final String standbyEndpoint = DEFAULT_HOST + ":" + standbySiteCorfuPort;

    final int numWrites = 2;

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    Process activeCorfu = null;
    Process standbyCorfu = null;

    Process activeReplicationServer = null;
    Process standbyReplicationServer = null;

    static final long sleepTime = 10;

    public void testLogReplicationEndToEnd(boolean useNetty, boolean runProcess) throws Exception {

        try {
            // Start Single Corfu Node Cluster on Active Site
            activeCorfu = runServer(activeSiteCorfuPort, true);

            // Start Corfu Cluster on Standby Site
            standbyCorfu = runServer(standbySiteCorfuPort, true);

            CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                    .builder()
                    .build();

            activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            activeRuntime.parseConfigurationString(activeEndpoint);
            activeRuntime.connect();

            activeRuntime1 = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            activeRuntime1.parseConfigurationString(activeEndpoint);
            activeRuntime1.connect();

            standbyRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
            standbyRuntime.parseConfigurationString(standbyEndpoint).connect();

            // Write to StreamA on Active Site
            mapA = activeRuntime.getObjectsView()
                    .build()
                    .setStreamName(streamA)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            mapAStandby = standbyRuntime.getObjectsView()
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

            System.out.print("\nnumWrites " + numWrites + " standyTail " +
                    standbyRuntime.getAddressSpaceView().getLogTail() + " activeTail " +
                    activeRuntime.getAddressSpaceView().getLogTail());

            if (runProcess) {
                // Start Log Replication Server on Active Site
                activeReplicationServer = runReplicationServer(activeReplicationServerPort);

                // Start Log Replication Server on Standby Site
                standbyReplicationServer = runReplicationServer(standbyReplicationServerPort);
            } else {
                serverA = new CorfuInterClusterReplicationServer(new String[]{"9010"});
                serverA.setSiteManagerAdapter(new DefaultClusterManager(2));
                Thread siteAThread = new Thread(serverA);
                System.out.print("\nStart Corfu Log Replication Server on 9010");
                siteAThread.start();

                serverB = new CorfuInterClusterReplicationServer(new String[]{"9020"});
                serverB.setSiteManagerAdapter(new DefaultClusterManager(2));
                Thread siteBThread = new Thread(serverB);
                System.out.print("\nStart Corfu Log Replication Server on 9020");
                siteBThread.start();
            }

            // Wait until data is fully replicated
            System.out.println("\nWait ... Log Entry Replication in progress ...");
            System.out.print("\nnumWrites done " + numWrites + " standyTail " + standbyRuntime.getAddressSpaceView().getLogTail() + " activeTail " +
                    activeRuntime.getAddressSpaceView().getLogTail());

            int retry = 0;
            while (mapAStandby.size() != numWrites && retry++ < MAX_RETRY) {
                System.out.print("\nmapStandySize " + mapAStandby.size() + " numWrites " + numWrites);
                System.out.print("\nstandyTail " + standbyRuntime.getAddressSpaceView().getLogTail() + " activeTail " +
                        activeRuntime.getAddressSpaceView().getLogTail());
                sleep(sleepInterval);
            }
            System.out.print("\nmapStandySize " + mapAStandby.size() + " numWrites " + numWrites);
            System.out.print("\nstandyTail " + standbyRuntime.getAddressSpaceView().getLogTail() + " activeTail " +
                    activeRuntime.getAddressSpaceView().getLogTail());

            // Verify data is present in Standby Site
            assertThat(mapAStandby.size()).isEqualTo(numWrites);

            System.out.print("\n Will add more data to the active: standyTail " + standbyRuntime.getAddressSpaceView().getLogTail() + " activeTail " +
                    activeRuntime.getAddressSpaceView().getLogTail());

            // Add new updates (deltas)
            for (int i = 0; i < numWrites / 2; i++) {
                activeRuntime.getObjectsView().TXBegin();
                mapA.put(String.valueOf(numWrites + i), numWrites + i);
                activeRuntime.getObjectsView().TXEnd();
            }

            System.out.print("\nAdded more data to the active: standyTail " + standbyRuntime.getAddressSpaceView().getLogTail() + " activeTail " +
                    activeRuntime.getAddressSpaceView().getLogTail());

            // Verify data is present in Standby Site
            System.out.println("\nWait ... Delta log replication in progress ...");
            while (mapAStandby.size() != mapA.size()) {
            }

            System.out.print("\nmapStandySize " + mapAStandby.size() + " mapASize " + mapA.size());
            System.out.print("\nstandyTail " + standbyRuntime.getAddressSpaceView().getLogTail() + " activeTail " +
                    activeRuntime.getAddressSpaceView().getLogTail());

            // Verify data is present in Standby Site (delta)
            assertThat(mapAStandby.size()).isEqualTo(numWrites + numWrites / 2);

            for (int i = 0; i < (numWrites + numWrites / 2); i++) {
                assertThat(mapAStandby.containsKey(String.valueOf(i)));
            }


            System.out.print("\nTest succeeds without site switch");

        } finally {

            if (!runProcess)
                return;

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
