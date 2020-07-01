package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuReplicationClusterConfigIT extends AbstractIT {
    private final static long sleepInterval = 1L;
    private final static String streamName = "Table001";
    private final static int firstBatch = 10;
    private final static int secondBatch = 15;

    private final static int activeSiteCorfuPort = 9000;
    private final static int standbySiteCorfuPort = 9001;
    private final static int activeReplicationServerPort = 9010;
    private final static int standbyReplicationServerPort = 9020;
    private final static String activeCorfuEndpoint = DEFAULT_HOST + ":" + activeSiteCorfuPort;
    private final static String standbyCorfuEndpoint = DEFAULT_HOST + ":" + standbySiteCorfuPort;

    private CorfuInterClusterReplicationServer activeReplicationServer;
    private CorfuInterClusterReplicationServer standbyReplicationServer;
    private CorfuTable<String, Integer> mapActive;
    private CorfuTable<String, Integer> mapStandby;
    private CorfuRuntime activeRuntime;
    private CorfuRuntime standbyRuntime;

    private Process activeCorfuServer = null;
    private Process standbyCorfuServer = null;
    private Thread activeReplicationServerThread = null;
    private Thread standbyReplicationServerThread = null;


    @After
    public void tearDown() {
        if (activeReplicationServer != null) {
            activeReplicationServer.cleanShutdown();
        }

        if (standbyReplicationServer != null) {
            standbyReplicationServer.cleanShutdown();
        }

        if (activeRuntime != null) {
            activeRuntime.shutdown();
        }

        if (standbyRuntime != null) {
            standbyRuntime.shutdown();
        }
    }

    @Test
    public void testLogReplicationRoleFlip() throws Exception {

        activeCorfuServer = runServer(activeSiteCorfuPort, true);
        standbyCorfuServer = runServer(standbySiteCorfuPort, true);

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        activeRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
        activeRuntime.parseConfigurationString(activeCorfuEndpoint).connect();

        standbyRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true);
        standbyRuntime.parseConfigurationString(standbyCorfuEndpoint).connect();

        mapActive = activeRuntime.getObjectsView()
                .build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        mapStandby = standbyRuntime.getObjectsView()
                .build()
                .setStreamName(streamName)
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .open();

        assertThat(mapActive.size()).isZero();
        assertThat(mapStandby.size()).isZero();

        // Write 10 entry to active map
        for (int i = 0; i < firstBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }
        assertThat(mapActive.size()).isEqualTo(firstBatch);

        System.out.println("First batch size is " + firstBatch + ", current standby tail is " +
                standbyRuntime.getAddressSpaceView().getLogTail() + ", and active tail is " +
                activeRuntime.getAddressSpaceView().getLogTail());

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.submit(() -> {
            CorfuInterClusterReplicationServer.main(new String[]{"test", "--address=localhost", String.valueOf(activeReplicationServerPort)});
        });

        executorService.submit(() -> {
            CorfuInterClusterReplicationServer.main(new String[]{"test",  "--address=localhost", String.valueOf(standbyReplicationServerPort)});
        });

        // Start Log Replication Server on Active Site
        activeReplicationServer = new CorfuInterClusterReplicationServer(new String[]{"--address=localhost", String.valueOf(activeReplicationServerPort)});
        activeReplicationServerThread = new Thread(activeReplicationServer);
        activeReplicationServerThread.start();
        // Start Log Replication Server on Standby Site
        standbyReplicationServer = new CorfuInterClusterReplicationServer(new String[]{"--address=localhost", String.valueOf(standbyReplicationServerPort)});
        standbyReplicationServerThread = new Thread(standbyReplicationServer);
        standbyReplicationServerThread.start();
        System.out.println("Replication servers started, and replication is in progress ...");

        // Wait until data is fully replicated
        waitForReplication(size -> size == firstBatch, mapStandby);

        System.out.println("After full sync, current standby tail is " +
                standbyRuntime.getAddressSpaceView().getLogTail() + ", and active tail is " +
                activeRuntime.getAddressSpaceView().getLogTail());

        // Add new updates (deltas)
        for (int i = firstBatch; i < firstBatch + secondBatch; i++) {
            activeRuntime.getObjectsView().TXBegin();
            mapActive.put(String.valueOf(i), i);
            activeRuntime.getObjectsView().TXEnd();
        }

        assertThat(mapActive.size()).isEqualTo(firstBatch + secondBatch);

        // Wait until data is fully replicated again
        waitForReplication(size -> size == firstBatch + secondBatch, mapStandby);

        System.out.println("After delta sync, current standby tail is " +
                standbyRuntime.getAddressSpaceView().getLogTail() + ", and active tail is " +
                activeRuntime.getAddressSpaceView().getLogTail());

        for (int i = 0; i < firstBatch + secondBatch; i++) {
            assertThat(mapStandby.containsKey(String.valueOf(i))).isTrue();
        }

        System.out.println("Test succeeds without site switch");

//        // Perform a role switch
//        DefaultClusterManager siteManagerA = (DefaultClusterManager) serverA.getClusterManagerAdapter();
//        DefaultClusterManager siteManagerB = (DefaultClusterManager) serverB.getClusterManagerAdapter();
//
//        TopologyDescriptor topologyDescriptorA = new TopologyDescriptor(siteManagerA.getTopologyConfig());
//        TopologyDescriptor topologyDescriptorB = new TopologyDescriptor(siteManagerB.getTopologyConfig());
//
//        assertThat(topologyDescriptorA.getActiveCluster().getClusterId())
//                .isEqualTo(topologyDescriptorB.getActiveCluster().getClusterId());
//        String activeId = topologyDescriptorA.getActiveCluster().getClusterId();
//        System.out.println("Before role switch, current active site id is: " + activeId);
//
//        siteManagerA.prepareSiteRoleChange();
//        int replicationStatus = siteManagerA.queryReplicationStatus();
//        System.out.println("Prepare role switch, replication status is " + replicationStatus);
//
//        System.out.println("Before site switch mapAstandby size " + mapAStandby.size() + " tail " + standbyRuntime.getAddressSpaceView().getLogTail() +
//                " mapA size " + mapA.size() + " tail " + activeRuntime.getAddressSpaceView().getLogTail());

        executorService.shutdownNow();

        //activeRuntime.shutdown();
       // standbyRuntime.shutdown();

        shutdownCorfuServer(activeCorfuServer);
        shutdownCorfuServer(standbyCorfuServer);


        System.out.println("Shutdown...");
    }

    private void waitForReplication(IntPredicate verifier, CorfuTable table) {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (verifier.test(table.size())) {
                break;
            }
            sleepUninterruptibly(sleepInterval);
        }
        assertThat(verifier.test(table.size())).isTrue();
    }

    private void sleepUninterruptibly(long seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }
}