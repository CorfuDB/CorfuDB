package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.infrastructure.logreplication.infrastructure.TopologyDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.junit.Test;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

public class CorfuReplicationSiteConfigIT extends AbstractIT {
    final static int MAX_RETRY = 10;
    final static long sleepInterval = 10000;
    private final String pluginConfigFilePath = "src/test/resources/transport/nettyConfig.properties";

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
    final int largeNumWrites = 10;

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
                serverA = new CorfuInterClusterReplicationServer(new String[]{"-m", "--plugin=" + pluginConfigFilePath, "--address=" + "localhost", String.valueOf(activeReplicationServerPort)});
                Thread siteAThread = new Thread(serverA);
                System.out.print("\nStart Corfu Log Replication Server on 9010");
                siteAThread.start();

                serverB = new CorfuInterClusterReplicationServer(new String[]{"-m", "--plugin=" + pluginConfigFilePath, "--address=" + "localhost", String.valueOf(standbyReplicationServerPort)});
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

    public void runNetty() throws Exception {
        testLogReplicationEndToEnd(true, false);
    }

    public void runCustomRouter() throws Exception {
        testLogReplicationEndToEnd(false, false);
    }

    public void shutdown() {
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

        if (serverA != null) {
            serverA.cleanShutdown();
        }

        if (serverB != null) {
            serverB.cleanShutdown();
        }
    }

    public void testPrepareRoleChangeAndQueryStatusAPI() throws Exception {
        try {
            testLogReplicationEndToEnd(false, false);

            //as data have been transfered over, the replication status should be 100% done.
            int replicationStatus = 0;
            DefaultClusterManager siteManager = (DefaultClusterManager) serverA.getClusterManagerAdapter();
            siteManager.prepareSiteRoleChange();
            replicationStatus = siteManager.queryReplicationStatus();
            while (replicationStatus != CorfuReplicationManager.PERCENTAGE_BASE) {
                replicationStatus = siteManager.queryReplicationStatus();
                System.out.print("\nreplication percentage done " + replicationStatus);
                sleep(sleepTime);
            }
            System.out.print("\nreplication percentage done " + replicationStatus);


            //pump some data to the active Site, and query the progress
            // Add new updates (deltas)
            for (int i = 0; i < largeNumWrites; i++) {
                activeRuntime.getObjectsView().TXBegin();
                mapA.put(String.valueOf(numWrites + i), numWrites + i);
                activeRuntime.getObjectsView().TXEnd();
            }

            replicationStatus = 0;
            siteManager.prepareSiteRoleChange();
            int retry = 0;
            while (replicationStatus != CorfuReplicationManager.PERCENTAGE_BASE && retry++ < MAX_RETRY) {
                replicationStatus = siteManager.queryReplicationStatus();
                System.out.print("\nreplication percentage done " + replicationStatus);
                sleep(sleepTime);
            }

            System.out.print("\nmapAStandby size " + mapAStandby.size() + " mapA size " + mapA.size());
            assertThat(mapAStandby.size()).isEqualTo(mapA.size());
        } finally {
            shutdown();
        }
    }

    @Test
    public void runSiteSwitch() throws Exception {
        try {
            testLogReplicationEndToEnd(false, false);

            int replicationStatus = 0;
            DefaultClusterManager siteManager = (DefaultClusterManager) serverA.getClusterManagerAdapter();
            siteManager.prepareSiteRoleChange();
            replicationStatus = siteManager.queryReplicationStatus();

            System.out.print("\nreplication percentage done " + replicationStatus);
            assertThat(replicationStatus).isEqualTo(CorfuReplicationManager.PERCENTAGE_BASE);


            TopologyDescriptor topologyDescriptor = new TopologyDescriptor(serverA.getClusterManagerAdapter().getTopologyConfig());

            String active = topologyDescriptor.getActiveClusters().keySet().iterator().next();
            String currentActive = active;

            // Wait till site role change and new transfer done.
            assertThat(currentActive).isEqualTo(active);

            System.out.print("\nbefore site switch mapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            siteManager = (DefaultClusterManager) serverA.getClusterManagerAdapter();
            siteManager.getSiteManagerCallback().clusterRoleChange = true;
            siteManager = (DefaultClusterManager) serverB.getClusterManagerAdapter();
            siteManager.getSiteManagerCallback().clusterRoleChange = true;

            CorfuReplicationDiscoveryService discoveryService = serverA.getReplicationDiscoveryService();
            synchronized (discoveryService) {
                discoveryService.wait();
            }

            topologyDescriptor = new TopologyDescriptor(serverA.getClusterManagerAdapter().getTopologyConfig());

            currentActive = topologyDescriptor.getActiveClusters().keySet().iterator().next();

            assertThat(currentActive).isNotEqualTo(active);
            System.out.print("\nVerified Site Role Change primary " + currentActive);
            System.out.print("\nmapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            CorfuTable<String, Integer> mapA1 = activeRuntime1.getObjectsView()
                    .build()
                    .setStreamName(streamA)
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .open();

            //block for snapshot transfer.
            sleep(sleepInterval);

            System.out.print("\nwriting to the new active new data for log entry transfer");
            for (int i = 0; i < largeNumWrites; i++) {
                standbyRuntime.getObjectsView().TXBegin();
                mapAStandby.put(String.valueOf(i), i);
                standbyRuntime.getObjectsView().TXEnd();
            }

            // Verify data is present in Standby Site
            System.out.println("\nWait ... Data is being replicated ...");
            System.out.print("\nmapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            replicationStatus = 0;

            siteManager = (DefaultClusterManager)serverB.getClusterManagerAdapter();
            sleep(sleepInterval);

            siteManager.prepareSiteRoleChange();
            while (replicationStatus != CorfuReplicationManager.PERCENTAGE_BASE) {
                replicationStatus = siteManager.queryReplicationStatus();
                System.out.print("\nreplication percentage done " + replicationStatus);
                sleep(sleepInterval);
            }

            while (mapA1.keySet().size() != mapAStandby.keySet().size()) {
                sleep(sleepInterval);
                System.out.print("\nmapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                        " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());
            }

            System.out.print("\nmapA1 keySet " + mapA1.keySet().size() + " mapAstandby " + mapAStandby.keySet().size());

            System.out.print("\nmapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
                    " mapAsize " + mapA1.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

            for (int i = 0; i < largeNumWrites; i++) {
               assertThat(mapA1.containsKey(String.valueOf(i))).isTrue();
            }

        } finally {
            shutdown();
        }
    }
}

