package org.corfudb.runtime.checkpoint;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.*;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.ILivenessUpdater;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class DistributedCompactorTests extends AbstractViewTest {

    private CorfuRuntime runtime0 = null;
    private CorfuRuntime runtime1 = null;
    private CorfuRuntime runtime2 = null;
    private CorfuRuntime cpRuntime0 = null;
    private CorfuRuntime cpRuntime1 = null;
    private CorfuRuntime cpRuntime2 = null;

    private CorfuStore corfuStore = null;

    private static final int WAIT_FOR_FINISH_CYCLE = 10;
    private static final int WAIT_IN_SYNC_STATE = 5000;
    private static final int LIVENESS_TIMEOUT = 1000;
    private static final String CLIENT_NAME_PREFIX = "Client";
    private static String STREAM_NAME = "streamNameA";

    /**
     * Generates and bootstraps a 3 node cluster in disk mode.
     *
     * @return The generated layout.
     */
    private Layout setup3NodeCluster() {
        ServerContext sc0 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_0))
                .setPort(SERVERS.PORT_0)
                .setMemory(false)
                .setCacheSizeHeapRatio("0.0")
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build();
        ServerContext sc1 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_1))
                .setPort(SERVERS.PORT_1)
                .setMemory(false)
                .setCacheSizeHeapRatio("0.0")
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build();
        ServerContext sc2 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_2))
                .setPort(SERVERS.PORT_2)
                .setMemory(false)
                .setCacheSizeHeapRatio("0.0")
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .build();

        addServer(SERVERS.PORT_0, sc0);
        addServer(SERVERS.PORT_1, sc1);
        addServer(SERVERS.PORT_2, sc2);
        Layout l = new TestLayoutBuilder()
                .setEpoch(0L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .setReplicationMode(Layout.ReplicationMode.CHAIN_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();

        bootstrapAllServers(l);

        // Shutdown management servers.
        getManagementServer(SERVERS.PORT_0).shutdown();
        getManagementServer(SERVERS.PORT_1).shutdown();
        getManagementServer(SERVERS.PORT_2).shutdown();

        return l;
    }

    @Before
    public void testSetup() {
        Layout l = setup3NodeCluster();

        runtime0 = getRuntime(l).connect();
        runtime1 = getRuntime(l).connect();
        runtime2 = getRuntime(l).connect();
        runtime0.getParameters().setClientName(CLIENT_NAME_PREFIX + "0");
        runtime1.getParameters().setClientName(CLIENT_NAME_PREFIX + "1");
        runtime2.getParameters().setClientName(CLIENT_NAME_PREFIX + "2");

        cpRuntime0 = getRuntime(l).connect();
        cpRuntime1 = getRuntime(l).connect();
        cpRuntime2 = getRuntime(l).connect();
        cpRuntime0.getParameters().setClientName(CLIENT_NAME_PREFIX + "_cp0");
        cpRuntime1.getParameters().setClientName(CLIENT_NAME_PREFIX + "_cp1");
        cpRuntime2.getParameters().setClientName(CLIENT_NAME_PREFIX + "_cp2");


        corfuStore = new CorfuStore(runtime0);
    }

    private Table<StringKey, CheckpointingStatus, Message> openCompactionManagerTable() {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    StringKey.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));
        } catch (Exception e) {
            log.error("Exception while opening tables ", e);
            return null;
        }
    }

    private Table<TableName, CheckpointingStatus, Message> openCheckpointStatusTable() {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT_STATUS_TABLE_NAME,
                    TableName.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));
        } catch (Exception e) {
            log.error("Exception while opening tables ", e);
            return null;
        }
    }

    private Table<StringKey, TokenMsg, Message> openCheckpointTable() {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT,
                    StringKey.class,
                    TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(TokenMsg.class));
        } catch (Exception e) {
            log.error("Exception while opening tables ", e);
            return null;
        }
    }

    private Table<TableName, ActiveCPStreamMsg, Message> openActiveCheckpointsTable() {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.ACTIVE_CHECKPOINTS_TABLE_NAME,
                    TableName.class,
                    ActiveCPStreamMsg.class,
                    null,
                    TableOptions.fromProtoSchema(ActiveCPStreamMsg.class));
        } catch (Exception e) {
            log.error("Exception while opening tables ", e);
            return null;
        }
    }

    private boolean verifyManagerStatus(StatusType targetStatus) {
        openCompactionManagerTable();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();
            if (managerStatus.getStatus() == targetStatus) {
                return true;
            }
        }
        return false;
    }

    private boolean verifyCheckpointStatusTable(StatusType targetStatus, int maxFailedTables) {

        Table<TableName, CheckpointingStatus, Message> cpStatusTable = openCheckpointStatusTable();

        int failed = 0;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            List<TableName> tableNames = new ArrayList<>(txn.keySet(cpStatusTable)
                    .stream().collect(Collectors.toList()));
            for (TableName table : tableNames) {
                CheckpointingStatus cpStatus = (CheckpointingStatus) txn.getRecord(
                        DistributedCompactor.CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                if (cpStatus.getStatus() != targetStatus) {
                    failed++;
                }
            }
            return failed <= maxFailedTables;
        }
    }

    private boolean verifyCheckpointTable() {

        openCheckpointTable();

        TokenMsg token;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            token = (TokenMsg) txn.getRecord(DistributedCompactor.CHECKPOINT, DistributedCompactor.CHECKPOINT_KEY).getPayload();
            txn.commit();
        }

        return (token != null);
    }

    @Test
    public void initTest() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime0, SERVERS.ENDPOINT_0);
        compactorLeaderServices1.setLeader(true);
        CompactorLeaderServices compactorLeaderServices2 = new CompactorLeaderServices(runtime1, SERVERS.ENDPOINT_1);
        compactorLeaderServices2.setLeader(false);

        assert(compactorLeaderServices1.trimAndTriggerDistributedCheckpointing());
        assert(!compactorLeaderServices2.trimAndTriggerDistributedCheckpointing());
        assert(verifyManagerStatus(StatusType.STARTED));
        assert(verifyCheckpointStatusTable(StatusType.IDLE, 0));
    }

    @Test
    public void initMultipleLeadersTest1() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime0, SERVERS.ENDPOINT_0);
        compactorLeaderServices1.setLeader(true);
        CompactorLeaderServices compactorLeaderServices2 = new CompactorLeaderServices(runtime1, SERVERS.ENDPOINT_1);
        compactorLeaderServices2.setLeader(true);

        boolean init1 = compactorLeaderServices1.trimAndTriggerDistributedCheckpointing();
        boolean init2 = compactorLeaderServices2.trimAndTriggerDistributedCheckpointing();

        assert (init1 ^ init2);
        assert(verifyManagerStatus(StatusType.STARTED));
        assert(verifyCheckpointStatusTable(StatusType.IDLE, 0));
    }

    @Test
    public void initMultipleLeadersTest2() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime0, SERVERS.ENDPOINT_0);
        compactorLeaderServices1.setLeader(true);
        CompactorLeaderServices compactorLeaderServices2 = new CompactorLeaderServices(runtime1, SERVERS.ENDPOINT_1);
        compactorLeaderServices2.setLeader(true);

        ExecutorService scheduler = Executors.newFixedThreadPool(2);
        Future<Boolean> future1 = scheduler.submit(() -> compactorLeaderServices1.trimAndTriggerDistributedCheckpointing());
        Future<Boolean> future2 = scheduler.submit(() -> compactorLeaderServices2.trimAndTriggerDistributedCheckpointing());
        compactorLeaderServices1.setLeader(false);

        try {
            assert(!(future1.get() && future2.get()));
            if (future1.get() ^ future1.get()) {
                assert (verifyManagerStatus(StatusType.STARTED));
                assert (verifyCheckpointStatusTable(StatusType.IDLE, 0));
            }
        } catch (Exception e) {
            log.warn("Unable to get results");
        }
    }

    @Test
    public void startCheckpointingTest() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime0, SERVERS.ENDPOINT_0);
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.trimAndTriggerDistributedCheckpointing();
        DistributedCompactor distributedCompactor1 =
                new DistributedCompactor(runtime0, cpRuntime0, null);
        DistributedCompactor distributedCompactor2 =
                new DistributedCompactor(runtime1, cpRuntime1, null);
        DistributedCompactor distributedCompactor3 =
                new DistributedCompactor(runtime2, cpRuntime2, null);

        int count1 = distributedCompactor1.startCheckpointing();
        int count2 = distributedCompactor2.startCheckpointing();
        int count3 = distributedCompactor3.startCheckpointing();
        int total = count1 + count2 + count3;

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            //This assert ensures each table is checkpointed by only one of the clients
            Assert.assertEquals(txn.count(DistributedCompactor.CHECKPOINT_STATUS_TABLE_NAME), total);
            txn.commit();
        }
        assert(verifyManagerStatus(StatusType.STARTED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
    }

    @Test
    public void finishCompactionCycleSuccessTest() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime0, SERVERS.ENDPOINT_0);
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.trimAndTriggerDistributedCheckpointing();

        DistributedCompactor distributedCompactor = new DistributedCompactor(runtime0, cpRuntime0, null);
        distributedCompactor.startCheckpointing();

        compactorLeaderServices1.finishCompactionCycle();

        assert(verifyManagerStatus(StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
    }

    @Test
    public void finishCompactionCycleFailureTest() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime0, SERVERS.ENDPOINT_0);
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.trimAndTriggerDistributedCheckpointing();

        //Checkpointing not done
        compactorLeaderServices1.finishCompactionCycle();

        assert(verifyManagerStatus(StatusType.FAILED));
        assert(verifyCheckpointStatusTable(StatusType.IDLE, 0));
    }

    private boolean pollForFinishCheckpointing() {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();
            txn.commit();
            log.info("managerStatus in test: {}", (managerStatus == null ? "null" : managerStatus.getStatus()));
            if (managerStatus != null && (managerStatus.getStatus() == StatusType.COMPLETED
                    || managerStatus.getStatus() == StatusType.FAILED)) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void validateLivenessLeaderTest() {
        //make some client to start cp
        //verifyCheckpointStatusTable
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime0, SERVERS.ENDPOINT_0);
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.trimAndTriggerDistributedCheckpointing();

        DistributedCompactor distributedCompactor = new DistributedCompactor(runtime0, cpRuntime0, null);
        distributedCompactor.startCheckpointing();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> compactorLeaderServices1.validateLiveness(LIVENESS_TIMEOUT), 0,
                LIVENESS_TIMEOUT, TimeUnit.MILLISECONDS);
        try {
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(WAIT_FOR_FINISH_CYCLE);
            }
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted, ", e);
        }

        assert(verifyManagerStatus(StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
    }

    @Test
    public void validateLivenessNonLeaderTest() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime0, SERVERS.ENDPOINT_0);
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.trimAndTriggerDistributedCheckpointing();
        compactorLeaderServices1.setLeader(false);

        DistributedCompactor distributedCompactor = new DistributedCompactor(runtime0, cpRuntime0, null);
        distributedCompactor.startCheckpointing();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> compactorLeaderServices1.validateLiveness(LIVENESS_TIMEOUT), 0,
                LIVENESS_TIMEOUT, TimeUnit.MILLISECONDS);

        try {
            TimeUnit.MILLISECONDS.sleep(LIVENESS_TIMEOUT);
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted, ", e);
        }

        assert(verifyManagerStatus(StatusType.STARTED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
    }

    @Test
    public void validateLivenessFailureTest() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime1, SERVERS.ENDPOINT_0);
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.trimAndTriggerDistributedCheckpointing();

        Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointTable = openActiveCheckpointsTable();
        Table<TableName, CheckpointingStatus, Message> checkpointStatusTable = openCheckpointStatusTable();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            TableName table = TableName.newBuilder().setNamespace(CORFU_SYSTEM_NAMESPACE).setTableName(STREAM_NAME).build();
            //Adding a table with STARTED value - making it look like someone started and died while checkpointing
            txn.putRecord(checkpointStatusTable, table,
                    CheckpointingStatus.newBuilder().setStatusValue(StatusType.STARTED_VALUE).build(), null);
            txn.putRecord(activeCheckpointTable,
                    table,
                    ActiveCPStreamMsg.getDefaultInstance(),
                    null);
            txn.commit();
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> compactorLeaderServices1.validateLiveness(LIVENESS_TIMEOUT), 0,
                LIVENESS_TIMEOUT, TimeUnit.MILLISECONDS);

        DistributedCompactor distributedCompactor = new DistributedCompactor(runtime0, cpRuntime0,
                null);
        distributedCompactor.startCheckpointing();

        try {
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(WAIT_FOR_FINISH_CYCLE);
            }
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted, ", e);
        }
        assert(verifyManagerStatus(StatusType.FAILED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 1));
    }

    private ILivenessUpdater mockLivenessUpdater = new ILivenessUpdater() {
        private ScheduledExecutorService executorService;
        private final static int updateInterval = 250;
        TableName tableName = null;

        @Override
        public void updateLiveness(TableName tableName) {
            this.tableName = tableName;
            // update validity counter every 250ms
            executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleWithFixedDelay(() -> {
                Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointsTable = openActiveCheckpointsTable();
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ActiveCPStreamMsg currentStatus =
                            txn.getRecord(activeCheckpointsTable, tableName).getPayload();
                    ActiveCPStreamMsg newStatus = ActiveCPStreamMsg.newBuilder()
                            .setSyncHeartbeat(currentStatus.getSyncHeartbeat() + 1)
                            .build();
                    txn.putRecord(activeCheckpointsTable, tableName, newStatus, null);
                    txn.commit();
                    log.info("Updated liveness for table {} to {}", tableName, currentStatus.getSyncHeartbeat() + 1);
                } catch (Exception e) {
                    log.error("Unable to update liveness for table: {}, e ", tableName, e);
                }
            }, 0, updateInterval, TimeUnit.MILLISECONDS);
        }

        private void changeStatus() {
            Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable = openCheckpointStatusTable();
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                CheckpointingStatus tableStatus =
                        txn.getRecord(checkpointingStatusTable, tableName).getPayload();
                if (tableStatus == null || tableStatus.getStatus() != StatusType.STARTED) {
                    txn.commit();
                    return;
                }
                CheckpointingStatus newStatus = CheckpointingStatus.newBuilder()
                        .setStatus(StatusType.COMPLETED)
                        .setClientName(tableStatus.getClientName())
                        .setTimeTaken(tableStatus.getTimeTaken())
                        .build();
                txn.putRecord(checkpointingStatusTable, tableName, newStatus, null);
                txn.delete(DistributedCompactor.ACTIVE_CHECKPOINTS_TABLE_NAME, tableName);
                txn.commit();
            } catch (Exception e) {
                log.error("Unable to mark status as COMPLETED for table: {}, {} StackTrace: {}",
                        tableName, e, e.getStackTrace());
            }
        }

        @Override
        public void notifyOnSyncComplete() {
            executorService.shutdownNow();
            changeStatus();
        }
    };

    @Test
    public void validateLivenessSyncStateTest() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime1, SERVERS.ENDPOINT_0);
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.trimAndTriggerDistributedCheckpointing();

        Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointTable = openActiveCheckpointsTable();
        Table<TableName, CheckpointingStatus, Message> checkpointStatusTable = openCheckpointStatusTable();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            TableName table = TableName.newBuilder().setNamespace(CORFU_SYSTEM_NAMESPACE).setTableName(STREAM_NAME).build();
            txn.putRecord(checkpointStatusTable, table,
                    CheckpointingStatus.newBuilder().setStatusValue(StatusType.STARTED_VALUE).build(), null);
            txn.putRecord(activeCheckpointTable,
                    table,
                    ActiveCPStreamMsg.getDefaultInstance(),
                    null);
            txn.commit();
            mockLivenessUpdater.updateLiveness(table);
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> compactorLeaderServices1.validateLiveness(LIVENESS_TIMEOUT), 0,
                LIVENESS_TIMEOUT, TimeUnit.MILLISECONDS);

        DistributedCompactor distributedCompactor = new DistributedCompactor(runtime0, cpRuntime0,
                null);

        try {
            TimeUnit.MILLISECONDS.sleep(WAIT_IN_SYNC_STATE);
            mockLivenessUpdater.notifyOnSyncComplete();

            distributedCompactor.startCheckpointing();
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(WAIT_FOR_FINISH_CYCLE);
            }
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted, ", e);
        }

        assert(verifyManagerStatus(StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
    }
}
