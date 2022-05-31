package org.corfudb.integration;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.lock.LockDataTypes;
import org.corfudb.runtime.ExampleSchemas.LockOp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.junit.Assert.fail;

@Slf4j
public class CorfuReplicationLeaderIT extends LogReplicationAbstractIT {

    private static final LockOp OP_FLIP = LockOp.newBuilder()
        .setOp("Flip Lock").build();

    private static final int activeReplicationServerPort1 = 9010;
    private static final int activeReplicationServerPort2 = 9011;
    private static final int standbyReplicationServerPort1 = 9020;
    private static final int standbyReplicationServerPort2 = 9021;
    private static final String LOCK_TABLE_NAME = "LOCK";
    private static final String LOCK_OP_TABLE_NAME = "LOCK_OP";
    private static final String LOCK_TABLE_TAG = "lock_data";

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

    private Process activeReplicationServer1;
    private Process activeReplicationServer2;
    private Process standbyReplicationServer1;
    private Process standbyReplicationServer2;

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

        activeRuntime1.shutdown();
        activeRuntime2.shutdown();
        standbyRuntime1.shutdown();
        standbyRuntime2.shutdown();

        shutdownCorfuServer(activeCorfuServer1);
        shutdownCorfuServer(standbyCorfuServer1);
        shutdownCorfuServer(activeReplicationServer1);
        shutdownCorfuServer(activeReplicationServer2);
        shutdownCorfuServer(standbyReplicationServer1);
        shutdownCorfuServer(standbyReplicationServer2);

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

        CountDownLatch activeCountdownLatch = new CountDownLatch(1);
        LockUpdateListener activeLockUpdateListener =
            new LockUpdateListener(activeCountdownLatch);
        activeCorfuStore1.subscribeListener(activeLockUpdateListener,
            CORFU_SYSTEM_NAMESPACE, LOCK_TABLE_TAG,
            Collections.singletonList(LOCK_TABLE_NAME));

        CountDownLatch standbyCountdownLatch = new CountDownLatch(1);
        LockUpdateListener standbyLockUpdateListener =
            new LockUpdateListener(standbyCountdownLatch);
        standbyCorfuStore1.subscribeListener(standbyLockUpdateListener,
            CORFU_SYSTEM_NAMESPACE, LOCK_TABLE_TAG,
            Collections.singletonList(LOCK_TABLE_NAME));

        activeReplicationServer1 = runReplicationServer(
            activeReplicationServerPort1, pluginConfigFilePath, lockLeaseDuration);
        activeCountdownLatch.await();

        activeReplicationServer2 = runReplicationServer(
            activeReplicationServerPort2, pluginConfigFilePath, lockLeaseDuration);

        // Start Standby LR
        standbyReplicationServer1 = runReplicationServer(
            standbyReplicationServerPort1, pluginConfigFilePath, lockLeaseDuration);
        standbyCountdownLatch.await();

        standbyReplicationServer2 = runReplicationServer(
            standbyReplicationServerPort2, pluginConfigFilePath, lockLeaseDuration);

        // Write to 2 tables on the active
        openMapsOnCluster(true, 2, 1, activeCorfuStore1);
        writeToMaps(true, 0, firstBatch, activeCorfuStore1);

        if (isSource) {
            activeCountdownLatch = new CountDownLatch(1);
            activeLockUpdateListener.updateCountdownLatch(activeCountdownLatch);
            shutdownCorfuServer(activeReplicationServer1);
            activeCountdownLatch.await();
        } else {
            standbyCountdownLatch = new CountDownLatch(1);
            standbyLockUpdateListener.updateCountdownLatch(standbyCountdownLatch);
            shutdownCorfuServer(standbyReplicationServer1);
            standbyCountdownLatch.await();
        }

        // More writes on the Source site
        openMapsOnCluster(true, 2, 1, activeCorfuStore2);
        writeToMaps(true, firstBatch, firstBatch, activeCorfuStore2);

        // Verify log entry sync completes
        openMapsOnCluster(false, 2, 1, standbyCorfuStore2);
        waitForReplication(size -> size == firstBatch*2, firstBatch*2);
    }

    @Test
    public void testConcurrentLeadershipChangeAndReplicationSourceFSM() throws Exception {
        testConcurrentLeadershipChangeAndReplicationFSM(true);
    }

    @Test
    public void testConcurrentLeadershipChangeAndReplicationSinkFSM() throws Exception {
        testConcurrentLeadershipChangeAndReplicationFSM(false);
    }

    private void testConcurrentLeadershipChangeAndReplicationFSM(boolean isSource)
        throws Exception {
        CountDownLatch activeCountdownLatch = new CountDownLatch(1);
        LockUpdateListener activeLockUpdateListener =
            new LockUpdateListener(activeCountdownLatch);
        activeCorfuStore1.subscribeListener(activeLockUpdateListener,
            CORFU_SYSTEM_NAMESPACE, LOCK_TABLE_TAG,
            Collections.singletonList(LOCK_TABLE_NAME));

        CountDownLatch standbyCountdownLatch = new CountDownLatch(1);
        LockUpdateListener standbyLockUpdateListener =
            new LockUpdateListener(standbyCountdownLatch);
        standbyCorfuStore1.subscribeListener(standbyLockUpdateListener,
            CORFU_SYSTEM_NAMESPACE, LOCK_TABLE_TAG,
            Collections.singletonList(LOCK_TABLE_NAME));

        activeReplicationServer1 = runReplicationServer(
            activeReplicationServerPort1, pluginConfigFilePath, lockLeaseDuration);
        activeReplicationServer2 = runReplicationServer(
            activeReplicationServerPort2, pluginConfigFilePath, lockLeaseDuration);

        // Start Standby LR
        standbyReplicationServer1 = runReplicationServer(
            standbyReplicationServerPort1, pluginConfigFilePath, lockLeaseDuration);
        standbyReplicationServer2 = runReplicationServer(
            standbyReplicationServerPort2, pluginConfigFilePath, lockLeaseDuration);

        log.info("Wait for lock acquisition");
        activeCountdownLatch.await();
        standbyCountdownLatch.await();
        log.info("Both clusters have a leader");

        openMapsOnCluster(true, 2, 1, activeCorfuStore1);
        scheduleConcurrently(f -> {
            for (int i=0; i < numIterations; i++) {
                writeToMaps(true,
                    i*firstBatch, firstBatch, activeCorfuStore1);
            }
        });

        CommonTypes.Uuid currentLeader = isSource ?
            activeLockUpdateListener.getCurrentLeaseOwner() :
            standbyLockUpdateListener.getCurrentLeaseOwner();
        log.info("Current leader is {}", currentLeader);

        CountDownLatch lockUpdateCountdownLatch = new CountDownLatch(1);
        LockUpdateListener updateListener;
        if (isSource) {
            activeLockUpdateListener.updateCountdownLatch(lockUpdateCountdownLatch);
            updateListener = activeLockUpdateListener;
            scheduleConcurrently(1, f -> {
                flipLeader(true);
                lockUpdateCountdownLatch.await();
            });
        } else {
            standbyLockUpdateListener.updateCountdownLatch(lockUpdateCountdownLatch);
            updateListener = standbyLockUpdateListener;
            scheduleConcurrently(1, f -> {
                flipLeader(false);
                lockUpdateCountdownLatch.await();
            });
        }
        executeScheduled(2, PARAMETERS.TIMEOUT_LONG);

        log.info("Verify that leadership flipped");
        //assertThat(updateListener.getAccumulatedStatus().size()).isEqualTo(2);
        CommonTypes.Uuid newLeader =
            updateListener.getCurrentLeaseOwner();
        assertThat(Objects.equals(newLeader, currentLeader)).isFalse();

        openMapsOnCluster(false, 2, 1, standbyCorfuStore1);
        waitForReplication(numWrites -> numWrites == firstBatch*numIterations,
            firstBatch*numIterations);
        activeCorfuStore1.unsubscribeListener(activeLockUpdateListener);
        standbyCorfuStore1.unsubscribeListener(standbyLockUpdateListener);
    }

    private void flipLeader(boolean isActive) {
        CorfuStore leaderCorfu = isActive ? activeCorfuStore1 : standbyCorfuStore1;
        Table<LockOp, LockOp, LockOp> lockTable =
            isActive ? activeLockOpTable1 : standbyLockOpTable1;

        TxnContext txnContext = leaderCorfu.txn(CORFU_SYSTEM_NAMESPACE);
        txnContext.putRecord(lockTable, OP_FLIP, OP_FLIP, null);
        txnContext.commit();
    }

    @Test
    public void testFlipLeadershipMultipleTimesSource() throws Exception {
        flipMultipleTimes(true, numIterations);
    }

    @Test
    public void testFlipLeadershipMultipleTimesSink() throws Exception {
        flipMultipleTimes(false, numIterations);
    }

    private void flipMultipleTimes(boolean isActive, int numIterations) throws Exception {
        CountDownLatch activeCountdownLatch = new CountDownLatch(1);
        LockUpdateListener activeLockUpdateListener =
            new LockUpdateListener(activeCountdownLatch);
        activeCorfuStore1.subscribeListener(activeLockUpdateListener,
            CORFU_SYSTEM_NAMESPACE, LOCK_TABLE_TAG,
            Collections.singletonList(LOCK_TABLE_NAME));

        CountDownLatch standbyCountdownLatch = new CountDownLatch(1);
        LockUpdateListener standbyLockUpdateListener =
            new LockUpdateListener(standbyCountdownLatch);
        standbyCorfuStore1.subscribeListener(standbyLockUpdateListener,
            CORFU_SYSTEM_NAMESPACE, LOCK_TABLE_TAG,
            Collections.singletonList(LOCK_TABLE_NAME));

        activeReplicationServer1 = runReplicationServer(
            activeReplicationServerPort1, pluginConfigFilePath, lockLeaseDuration);
        activeReplicationServer2 = runReplicationServer(
            activeReplicationServerPort2, pluginConfigFilePath, lockLeaseDuration);

        // Start Standby LR
        standbyReplicationServer1 = runReplicationServer(
            standbyReplicationServerPort1, pluginConfigFilePath, lockLeaseDuration);
        standbyReplicationServer2 = runReplicationServer(
            standbyReplicationServerPort2, pluginConfigFilePath, lockLeaseDuration);

        // Wait till both clusters get a leader
        activeCountdownLatch.await();
        standbyCountdownLatch.await();

        CommonTypes.Uuid currentLeader;
        CommonTypes.Uuid newLeader;
        if (isActive) {
            currentLeader =
                activeLockUpdateListener.getCurrentLeaseOwner();
            for (int i=0; i<numIterations; i++) {
                activeCountdownLatch = new CountDownLatch(1);
                activeLockUpdateListener.updateCountdownLatch(activeCountdownLatch);
                flipLeader(true);
                activeCountdownLatch.await();
                newLeader =
                    activeLockUpdateListener.getCurrentLeaseOwner();
                assertThat(Objects.equals(currentLeader, newLeader)).isFalse();
                currentLeader = newLeader;
            }
        } else {
            currentLeader =
                standbyLockUpdateListener.getCurrentLeaseOwner();
            for (int i=0; i<numIterations; i++) {
                standbyCountdownLatch = new CountDownLatch(1);
                standbyLockUpdateListener.updateCountdownLatch(standbyCountdownLatch);
                flipLeader(false);
                standbyCountdownLatch.await();
                newLeader =
                    standbyLockUpdateListener.getCurrentLeaseOwner();
                assertThat(Objects.equals(currentLeader, newLeader)).isFalse();
                currentLeader = newLeader;
            }
        }
    }

    @Test
    public void testSnapshotSyncTransitionAfterTrimAndLeaderUpdate() throws Exception {
        final int totalUpdatesOnStandbyReplicationTable = 4;

        CountDownLatch activeCountdownLatch = new CountDownLatch(1);
        LockUpdateListener activeLockUpdateListener =
            new LockUpdateListener(activeCountdownLatch);
        activeCorfuStore1.subscribeListener(activeLockUpdateListener,
            CORFU_SYSTEM_NAMESPACE, LOCK_TABLE_TAG,
            Collections.singletonList(LOCK_TABLE_NAME));
        activeReplicationServer1 = runReplicationServer(
            activeReplicationServerPort1, pluginConfigFilePath, lockLeaseDuration);

        activeReplicationServer2 = runReplicationServer(
            activeReplicationServerPort2, pluginConfigFilePath, lockLeaseDuration);

        activeCountdownLatch.await();

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
        standbyReplicationServer1 = runReplicationServer(
            standbyReplicationServerPort1, pluginConfigFilePath, lockLeaseDuration);

        standbyReplicationServer2 = runReplicationServer(
            standbyReplicationServerPort2, pluginConfigFilePath, lockLeaseDuration);

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

        CommonTypes.Uuid currentLeader =
            activeLockUpdateListener.getCurrentLeaseOwner();
        log.info("Current Leader {}", currentLeader);
        activeCountdownLatch = new CountDownLatch(1);
        activeLockUpdateListener.updateCountdownLatch(activeCountdownLatch);
        flipLeader(true);

        log.info("Wait for leader to change");
        activeCountdownLatch.await();

        log.info("Verify leader changed");
        CommonTypes.Uuid newLeader =
            activeLockUpdateListener.getCurrentLeaseOwner();
        assertThat(Objects.equals(currentLeader, newLeader)).isFalse();

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

    private class LockUpdateListener implements StreamListener {

        @Getter
        private List<CommonTypes.Uuid> accumulatedStatus = new ArrayList<>();

        private CountDownLatch countDownLatch;

        @Getter
        private CommonTypes.Uuid currentLeaseOwner = null;

        public LockUpdateListener(CountDownLatch countdownLatch) {
            this.countDownLatch = countdownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> entries.forEach(e ->
                accumulatedStatus.add(((LockDataTypes.LockData)e.getPayload()).getLeaseOwnerId())));
            CommonTypes.Uuid newOwner =
                accumulatedStatus.get(accumulatedStatus.size()-1);

            // Check if the owner changed and if so, decrement the latch
            if (!Objects.equals(currentLeaseOwner, newOwner)) {
                currentLeaseOwner = newOwner;
                countDownLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            fail("onError for LockUpdateListener");
        }

        void updateCountdownLatch(CountDownLatch newCountdownLatch) {
            countDownLatch = newCountdownLatch;
        }

    }
}
