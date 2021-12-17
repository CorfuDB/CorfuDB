package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.runtime.ExampleSchemas.LockOp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class CorfuReplicationLeaderIT extends LogReplicationAbstractIT {

    private static final LockOp OP_FLIP = LockOp.newBuilder()
        .setOp("Flip Lock").build();

    private static final int activeReplicationServerPort1 = 9010;
    private static final int activeReplicationServerPort2 = 9011;
    private static final int standbyReplicationServerPort1 = 9020;
    private static final int standbyReplicationServerPort2 = 9021;
    private static final String nettyPluginPath =
        "src/test/resources/transport/nettyConfig.properties";
    private static final String LOCK_TABLE_NAME = "LOCK";
    private static final String LOCK_OP_TABLE_NAME = "LOCK_OP";
    Table<LockOp, LockOp, LockOp> activeLockOpTable1;
    Table<LockOp, LockOp, LockOp> activeLockOpTable2;
    Table<LockOp, LockOp, LockOp> standbyLockOpTable1;
    Table<LockOp, LockOp, LockOp> standbyLockOpTable2;

    private static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";

    private Process activeCorfuServer1;
    private Process standbyCorfuServer1;

    CorfuRuntime activeRuntime1;
    CorfuRuntime activeRuntime2;
    CorfuRuntime standbyRuntime1;
    CorfuRuntime standbyRuntime2;

    private CorfuStore activeCorfuStore1;
    private CorfuStore activeCorfuStore2;
    private CorfuStore standbyCorfuStore1;
    private CorfuStore standbyCorfuStore2;

    private CorfuInterClusterReplicationServer activeServer1;
    private CorfuInterClusterReplicationServer activeServer2;
    private CorfuInterClusterReplicationServer standbyServer1;
    private CorfuInterClusterReplicationServer standbyServer2;

    public Map<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> mapNameToMapActive;
    public Map<String, Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>> mapNameToMapStandby;

    public static final String TABLE_PREFIX = "Table00";
    public static final String NAMESPACE = "LR-Test";
    private static final int firstBatch = 10;
    private static final int lockLeaseDuration = 20;
    private static final int numIterations = 5;

    @Before
    public void setUp() throws Exception {
        log.info("Setup Test Config");
        activeCorfuServer1 = runServer(activeSiteCorfuPort, true);
        standbyCorfuServer1 = runServer(standbySiteCorfuPort, true);

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
            .builder()
            .build();

        activeRuntime1 = CorfuRuntime.fromParameters(params);
        activeRuntime1.parseConfigurationString(activeEndpoint).connect();

        activeRuntime2 = CorfuRuntime.fromParameters(params);
        activeRuntime2.parseConfigurationString(activeEndpoint).connect();

        standbyRuntime1 = CorfuRuntime.fromParameters(params);
        standbyRuntime1.parseConfigurationString(standbyEndpoint).connect();

        standbyRuntime2 = CorfuRuntime.fromParameters(params);
        standbyRuntime2.parseConfigurationString(standbyEndpoint).connect();

        activeCorfuStore1 = new CorfuStore(activeRuntime1);
        activeCorfuStore2 = new CorfuStore(activeRuntime2);
        standbyCorfuStore1 = new CorfuStore(standbyRuntime1);
        standbyCorfuStore2 = new CorfuStore(standbyRuntime2);

        activeCorfuStore1.openTable(
            DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
            ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class,
            TableOptions.fromProtoSchema(ExampleSchemas.ClusterUuidMsg.class));
        activeCorfuStore2.openTable(
            DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
            ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class,
            TableOptions.fromProtoSchema(ExampleSchemas.ClusterUuidMsg.class));

        activeCorfuStore1.openTable(
            CORFU_SYSTEM_NAMESPACE,
            LOCK_TABLE_NAME,
            LockDataTypes.LockId.class,
            LockDataTypes.LockData.class,
            null,
            TableOptions.fromProtoSchema(LockDataTypes.LockData.class));
        activeLockOpTable1 = activeCorfuStore1.openTable(
            CORFU_SYSTEM_NAMESPACE,
            LOCK_OP_TABLE_NAME,
            LockOp.class,
            LockOp.class,
            null,
            TableOptions.fromProtoSchema(LockOp.class));

        activeCorfuStore2.openTable(
            CORFU_SYSTEM_NAMESPACE,
            LOCK_TABLE_NAME,
            LockDataTypes.LockId.class,
            LockDataTypes.LockData.class,
            null,
            TableOptions.fromProtoSchema(LockDataTypes.LockData.class));
        activeLockOpTable2 = activeCorfuStore2.openTable(
            CORFU_SYSTEM_NAMESPACE,
            LOCK_OP_TABLE_NAME,
            LockOp.class,
            LockOp.class,
            null,
            TableOptions.fromProtoSchema(LockOp.class));

        activeCorfuStore1.openTable(LogReplicationMetadataManager.NAMESPACE,
            REPLICATION_STATUS_TABLE,
            LogReplicationMetadata.ReplicationStatusKey.class,
            LogReplicationMetadata.ReplicationStatusVal.class,
            null,
            TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
        activeCorfuStore2.openTable(LogReplicationMetadataManager.NAMESPACE,
            REPLICATION_STATUS_TABLE,
            LogReplicationMetadata.ReplicationStatusKey.class,
            LogReplicationMetadata.ReplicationStatusVal.class,
            null,
            TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        standbyLockOpTable1 = standbyCorfuStore1.openTable(
            CORFU_SYSTEM_NAMESPACE,
            LOCK_OP_TABLE_NAME,
            LockOp.class,
            LockOp.class,
            null,
            TableOptions.fromProtoSchema(LockOp.class));
        standbyCorfuStore1.openTable(LogReplicationMetadataManager.NAMESPACE,
            REPLICATION_STATUS_TABLE,
            LogReplicationMetadata.ReplicationStatusKey.class,
            LogReplicationMetadata.ReplicationStatusVal.class,
            null,
            TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
        standbyCorfuStore1.openTable(
            CORFU_SYSTEM_NAMESPACE,
            LOCK_TABLE_NAME,
            LockDataTypes.LockId.class,
            LockDataTypes.LockData.class,
            null,
            TableOptions.fromProtoSchema(LockDataTypes.LockData.class));

        standbyLockOpTable2 = standbyCorfuStore2.openTable(
            CORFU_SYSTEM_NAMESPACE,
            LOCK_OP_TABLE_NAME,
            LockOp.class,
            LockOp.class,
            null,
            TableOptions.fromProtoSchema(LockOp.class)
        );
        standbyCorfuStore2.openTable(LogReplicationMetadataManager.NAMESPACE,
            REPLICATION_STATUS_TABLE,
            LogReplicationMetadata.ReplicationStatusKey.class,
            LogReplicationMetadata.ReplicationStatusVal.class,
            null,
            TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));
        standbyCorfuStore2.openTable(
            CORFU_SYSTEM_NAMESPACE,
            LOCK_TABLE_NAME,
            LockDataTypes.LockId.class,
            LockDataTypes.LockData.class,
            null,
            TableOptions.fromProtoSchema(LockDataTypes.LockData.class));
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        log.info("Tear Down Test Config");

        activeServer1.cleanShutdown();
        activeServer2.cleanShutdown();
        standbyServer1.cleanShutdown();
        standbyServer2.cleanShutdown();

        activeRuntime1.shutdown();
        activeRuntime2.shutdown();
        standbyRuntime1.shutdown();
        standbyRuntime2.shutdown();

        shutdownCorfuServer(activeCorfuServer1);
        shutdownCorfuServer(standbyCorfuServer1);

        executorService.shutdownNow();
    }

    @Test
    public void testLeadershipChangeOnSourceLRStop() throws Exception {
        testReplicationAfterLeadershipChangeOnLRStop(true);
    }

    @Test
    public void testLeadershipChangeOnSinkLRStop() throws Exception {
        testReplicationAfterLeadershipChangeOnLRStop(false);
    }

    private void testReplicationAfterLeadershipChangeOnLRStop(boolean isSource)
        throws Exception {

        activeServer1 = runFromArgs(activeReplicationServerPort1);

        activeServer2 = runFromArgs(activeReplicationServerPort2);

        // Start Standby LR
        standbyServer1 = runFromArgs(standbyReplicationServerPort1);

        standbyServer2 = runFromArgs(standbyReplicationServerPort2);
        sleepUninterruptibly(1);

        CorfuInterClusterReplicationServer leader;
        if (isSource) {
            leader = getCurrentLeader(activeServer1, activeServer2);
        } else {
            leader = getCurrentLeader(standbyServer1, standbyServer2);
        }

        // Write to 2 tables on the active
        openMapsOnCluster(true, 2, 1, activeCorfuStore1);
        writeToMaps(true, 0, firstBatch, activeCorfuStore1);

        leader.getReplicationDiscoveryService().shutdown();

        // More writes on the Source site
        openMapsOnCluster(true, 2, 1, activeCorfuStore2);
        writeToMaps(true, firstBatch, firstBatch, activeCorfuStore2);

        // Verify log entry sync completes
        openMapsOnCluster(false, 2, 1, standbyCorfuStore1);
        waitForReplication(size -> size == firstBatch*2, firstBatch*2);
    }

    @Test
    public void testConcurrentLeadershipChangeAndReplicationSourceFSM() throws Exception {
        testConcurrentLeadershipChangeAndReplicationFSM(false,true);
    }

    @Test
    public void testConcurrentLeadershipChangeAndReplicationSinkFSM() throws Exception {
        testConcurrentLeadershipChangeAndReplicationFSM(false,false);
    }

    @Test
    public void testConcurrentLeadershipChangeAndReplicationBothFSM() throws Exception {
        testConcurrentLeadershipChangeAndReplicationFSM(true, false);
    }

    private void testConcurrentLeadershipChangeAndReplicationFSM(boolean both, boolean isSource)
        throws Exception {

        activeServer1 = runFromArgs(activeReplicationServerPort1);
        activeServer2 = runFromArgs(activeReplicationServerPort2);

        // Start Standby LR
        standbyServer1 = runFromArgs(standbyReplicationServerPort1);
        standbyServer2 = runFromArgs(standbyReplicationServerPort2);
        sleepUninterruptibly(1);

        // Wait till lock table on both has data
        waitForLockAcquisition();

        openMapsOnCluster(true, 2, 1, activeCorfuStore1);
        scheduleConcurrently(f -> {
            for (int i=0; i < numIterations; i++) {
                writeToMaps(true,
                    i*firstBatch, firstBatch, activeCorfuStore1);
            }
        });

        if (both) {
            scheduleConcurrently(1, f -> {
                flipLeader(true, activeServer1, activeServer2);
                flipLeader(false, standbyServer1, standbyServer2);
            });
        } else if (isSource) {
            scheduleConcurrently(1, f -> {
                flipLeader(true, activeServer1, activeServer2);
            });
        } else {
            scheduleConcurrently(1, f -> {
                flipLeader(false, standbyServer1, standbyServer2);
            });
        }
        executeScheduled(2, PARAMETERS.TIMEOUT_LONG);
        log.info("Leader flipped.  Now waiting for replication");

        openMapsOnCluster(false, 2, 1, standbyCorfuStore1);
        waitForReplication(numWrites -> numWrites == firstBatch*numIterations,
            firstBatch*numIterations);
    }

    private CorfuInterClusterReplicationServer runFromArgs(int replicationPort) {
        String[] args = {"-m", "--address=localhost",
            "--plugin="+nettyPluginPath, "--lock-lease="+String.valueOf(lockLeaseDuration),
            String.valueOf(replicationPort)};

        CorfuInterClusterReplicationServer server =
            new CorfuInterClusterReplicationServer(args);
        CompletableFuture.runAsync(server);
        return server;
    }

    private void waitForLockAcquisition() {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (activeCorfuStore1.getTable(CORFU_SYSTEM_NAMESPACE,
                LOCK_TABLE_NAME).count() == 0 ||
                standbyCorfuStore1.getTable(CORFU_SYSTEM_NAMESPACE,
                    LOCK_TABLE_NAME).count() == 0) {
                sleepUninterruptibly(1);
            } else {
                log.info("LR clusters setup complete");
                break;
            }
        }
    }

    private void flipLeader(boolean isActive,
        CorfuInterClusterReplicationServer server1,
        CorfuInterClusterReplicationServer server2) {

        CorfuStore leaderCorfu = isActive ? activeCorfuStore1 : standbyCorfuStore1;
        Table<LockOp, LockOp, LockOp> lockTable =
            isActive ? activeLockOpTable1 : standbyLockOpTable1;
        CorfuInterClusterReplicationServer leaderLR;
        CorfuInterClusterReplicationServer nonLeaderLR;

        if (server1.getReplicationDiscoveryService().getIsLeader().get()) {
            leaderLR = server1;
            nonLeaderLR = server2;
        } else {
            leaderLR = server2;
            nonLeaderLR = server1;
        }

        TxnContext txnContext = leaderCorfu.txn(CORFU_SYSTEM_NAMESPACE);
        txnContext.putRecord(lockTable, OP_FLIP, OP_FLIP, null);
        txnContext.commit();

        // Verify that leadership flipped
        waitForLeadershipFlip(nonLeaderLR, leaderLR);
    }

    private void waitForLeadershipFlip(
        CorfuInterClusterReplicationServer expectedLeader,
        CorfuInterClusterReplicationServer expectedNonLeader) {

        for (int i=0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
            if (expectedLeader.getReplicationDiscoveryService().getIsLeader().get()
                && !expectedNonLeader.getReplicationDiscoveryService().getIsLeader().get()) {
                break;
            }
            sleepUninterruptibly(1);
        }
        Assert.assertTrue(expectedLeader.getReplicationDiscoveryService().getIsLeader().get());
        Assert.assertFalse(expectedNonLeader.getReplicationDiscoveryService().getIsLeader().get());
        log.info("Leadership Flip Successful");
    }

    private CorfuInterClusterReplicationServer getCurrentLeader(
        CorfuInterClusterReplicationServer server1,
        CorfuInterClusterReplicationServer server2) {

        return server1.getReplicationDiscoveryService().getIsLeader().get() ?
            server1 : server2;
    }

    @Test
    public void testFlipLeadershipMultipleTimesSource() {
        flipMultipleTimes(true, numIterations);
    }

    @Test
    public void testFlipLeadershipMultipleTimesSink() {
        flipMultipleTimes(false, numIterations);
    }

    private void flipMultipleTimes(boolean isActive, int numIterations) {
        activeServer1 = runFromArgs(activeReplicationServerPort1);

        activeServer2 = runFromArgs(activeReplicationServerPort2);

        // Start Standby LR
        standbyServer1 = runFromArgs(standbyReplicationServerPort1);

        standbyServer2 = runFromArgs(standbyReplicationServerPort2);
        sleepUninterruptibly(1);

        if (isActive) {
            for (int i=0; i<numIterations; i++) {
                flipLeader(true, activeServer1, activeServer2);
            }
        } else {
            for (int i=0; i<numIterations; i++) {
                flipLeader(false, standbyServer1, standbyServer2);
            }
        }
    }

  @Test
    public void testSnapshotSyncTransitionAfterTrimAndLeaderUpdate() throws Exception {
        final int totalUpdatesOnStandbyReplicationTable = 4;

        activeServer1 = runFromArgs(activeReplicationServerPort1);

        activeServer2 = runFromArgs(activeReplicationServerPort2);

        openMapsOnCluster(true, 2, 1, activeCorfuStore1);
        writeToMaps(true, 0, firstBatch, activeCorfuStore1);
        openMapsOnCluster(false, 2, 1, standbyCorfuStore1);

        // Subscribe the standby runtime to Replication Status table.
        // On snapshot sync, there will be 2 updates 1)setDataConsistent = false
        // and 2)setDataConsistent = true on the standby.
        CountDownLatch statusUpdateLatch = new CountDownLatch(2);
        ReplicationStatusListener standbyStatusListener =
            new ReplicationStatusListener(statusUpdateLatch);
        standbyCorfuStore1.subscribeListener(standbyStatusListener,
            LogReplicationMetadataManager.NAMESPACE,
            LogReplicationMetadataManager.LR_STATUS_STREAM_TAG);

        // Start Standby LR
        standbyServer1 = runFromArgs(standbyReplicationServerPort1);

        standbyServer2 = runFromArgs(standbyReplicationServerPort2);

        // Wait for replication and verify that snapshot sync completed
        waitForReplication(numWrites -> numWrites == firstBatch, firstBatch);
        statusUpdateLatch.await();
        assertThat(standbyStatusListener.getAccumulatedStatus().size()).isEqualTo(2);
        assertThat(standbyStatusListener.getAccumulatedStatus().get(0)).isFalse();
        assertThat(standbyStatusListener.getAccumulatedStatus().get(1)).isTrue();

        // It is expected that leadership change on the active cluster will
        // trigger a negotiation, leading to a Snapshot Sync due to CP+Trim.
        // Hence, there will be 2 updates 1)setDataConsistent = false and
        // 2)setDataConsistent = true on the standby.
        statusUpdateLatch = new CountDownLatch(2);
        standbyStatusListener.updateCountdownLatch(statusUpdateLatch);

        checkpointAndTrim(true);
        flipLeader(true, activeServer1, activeServer2);

        waitForReplication(numWrites -> numWrites == firstBatch, firstBatch);
        statusUpdateLatch.await();
        assertThat(standbyStatusListener.getAccumulatedStatus().size())
            .isEqualTo(totalUpdatesOnStandbyReplicationTable);
        assertThat(standbyStatusListener.getAccumulatedStatus().get(2)).isFalse();
        assertThat(standbyStatusListener.getAccumulatedStatus()
            .get(totalUpdatesOnStandbyReplicationTable - 1)).isTrue();
        log.info("Snapshot sync finished after leadership change");
        standbyCorfuStore1.unsubscribeListener(standbyStatusListener);
    }

    private void openMapsOnCluster(boolean isActive, int mapCount,
        int startIndex, CorfuStore corfuStore) throws Exception {
        mapNameToMapActive = new HashMap<>();
        mapNameToMapStandby = new HashMap<>();

        for(int i=startIndex; i <= mapCount; i++) {
            String mapName = TABLE_PREFIX + i;

            if (isActive) {
                Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapActive = corfuStore.openTable(
                    NAMESPACE, mapName, Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class));
                mapNameToMapActive.put(mapName, mapActive);
            } else {
                Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> mapStandby = corfuStore.openTable(
                    NAMESPACE, mapName, Sample.StringKey.class, Sample.IntValueTag.class, Sample.Metadata.class,
                    TableOptions.fromProtoSchema(Sample.IntValueTag.class));
                mapNameToMapStandby.put(mapName, mapStandby);
            }
        }
    }

    private void writeToMaps(boolean active, int startIndex, int totalEntries,
        CorfuStore corfuStore) {
        int maxIndex = totalEntries + startIndex;

        Map<String, Table<Sample.StringKey, Sample.IntValueTag,
            Sample.Metadata>> map;

        if (active) {
            map = mapNameToMapActive;
        } else {
            map = mapNameToMapStandby;
        }
        for (Map.Entry<String, Table<Sample.StringKey, Sample.IntValueTag,
            Sample.Metadata>> entry : map.entrySet()) {

            log.debug(">>> Write to cluster, map={}.  Active = {}",
                entry.getKey(), active);

            Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata> table =
                entry.getValue();
            for (int i = startIndex; i < maxIndex; i++) {
                Sample.StringKey stringKey = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
                Sample.IntValueTag intValueTag = Sample.IntValueTag.newBuilder().setValue(i).build();
                Sample.Metadata metadata = Sample.Metadata.newBuilder().setMetadata("Metadata_" + i).build();
                try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                    txn.putRecord(table, stringKey, intValueTag, metadata);
                    txn.commit();
                }
            }
            assertThat(table.count()).isEqualTo(maxIndex);
        }
    }

    private void waitForReplication(IntPredicate verifier, int expected) {
        for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>
            table : mapNameToMapStandby.values()) {
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
                log.info("Waiting for replication, table size is {} expected size is {}",
                    table.count(), expected);

                if (verifier.test(table.count())) {
                    break;
                }
                sleepUninterruptibly(1);
            }
        }
        for (Table<Sample.StringKey, Sample.IntValueTag, Sample.Metadata>
            table : mapNameToMapStandby.values()) {
            assertThat(verifier.test(table.count())).isTrue();
        }
    }

    private void sleepUninterruptibly(long seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ie) {
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }
}
