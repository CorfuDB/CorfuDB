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
    private static final int N_THREADS = 3;
    private static final int WAIT_FOR_FINISH_CYCLE = 10;
    private static final int LIVENESS_TIMEOUT = 1000;
    private static final int VALIDATE_LIVENESS_INTERVAL = 1000;

    private CorfuRuntime runtime1 = null;
    private CorfuRuntime runtime2 = null;
    private CorfuRuntime runtime3 = null;
    private CorfuRuntime cpRuntime1 = null;
    private CorfuRuntime cpRuntime2 = null;
    private CorfuRuntime cpRuntime3 = null;

    private CorfuStore corfuStore = null;

    private Map<String, Table<StringKey, StringKey, Message>> openedStreams = new HashMap<>();

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

        runtime1 = getRuntime(l).connect();
        runtime2 = getRuntime(l).connect();
        runtime3 = getRuntime(l).connect();
        cpRuntime1 = getRuntime(l).connect();
        cpRuntime2 = getRuntime(l).connect();
        cpRuntime3 = getRuntime(l).connect();

        corfuStore = new CorfuStore(runtime1);
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

    private Table<StringKey, StringKey, Message> openStream(CorfuStore corfuStore, String streamName) {
        try {
            Table<StringKey, StringKey, Message> table = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    streamName,
                    StringKey.class,
                    StringKey.class,
                    null,
                    TableOptions.fromProtoSchema(StringKey.class));
            openedStreams.put(streamName, table);
            return table;
        } catch (Exception e) {
            log.error("Exception while opening tables ", e);
        }
        return null;
    }

    private static String STREAM_KEY_PREFIX = "StreamKey";
    private static String STREAM_VALUE_PREFIX = "StreamValue";

    private void populateStream(String streamName, int numRecords) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            for (int i = 0; i < numRecords; i++) {
                txn.putRecord(openedStreams.get(streamName),
                        StringKey.newBuilder().setKey(STREAM_KEY_PREFIX + i).build(),
                        StringKey.newBuilder().setKey(STREAM_VALUE_PREFIX + i).build(),
                        null);
            }
            txn.commit();
        }
    }

    private boolean verifyManagerStatus(StatusType targetStatus) {
        openCompactionManagerTable();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();
            System.out.println("ManagerStatus: " + managerStatus.getStatus());
            if (managerStatus.getStatus() == targetStatus) {
                System.out.println("verifyManagerStatus: returning true");
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
                System.out.println(table.getTableName() +
                        " : " + cpStatus.getStatus() + " clientId: " + cpStatus.getClientName());
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

        if (token != null) {
            return true;
        }
        return false;
    }

    private static String STREAM_NAME_PREFIX = "streamName";
    private static final int NUM_RECORDS = 50;
    private static final int TIMEOUT = 45;

    @Test
    public void initTest() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime1, UUID.randomUUID());
        compactorLeaderServices1.setLeader(true);
        CompactorLeaderServices compactorLeaderServices2 = new CompactorLeaderServices(runtime2, UUID.randomUUID());
        compactorLeaderServices2.setLeader(false);

        assert(compactorLeaderServices1.triggerCheckpointing());
        assert(!compactorLeaderServices2.triggerCheckpointing());
        assert(verifyManagerStatus(StatusType.STARTED));
        assert(verifyCheckpointStatusTable(StatusType.IDLE, 0));
    }

    @Test
    public void initMultipleLeadersTest1() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime1, UUID.randomUUID());
        compactorLeaderServices1.setLeader(true);
        CompactorLeaderServices compactorLeaderServices2 = new CompactorLeaderServices(runtime2, UUID.randomUUID());
        compactorLeaderServices2.setLeader(true);
        CompactorLeaderServices compactorLeaderServices3 = new CompactorLeaderServices(runtime3, UUID.randomUUID());
        compactorLeaderServices3.setLeader(true);

        boolean init1 = compactorLeaderServices1.triggerCheckpointing();
        boolean init2 = compactorLeaderServices2.triggerCheckpointing();
        boolean init3 = compactorLeaderServices3.triggerCheckpointing();
        assert(init1 ^ init2 ^ init3);
        assert(verifyManagerStatus(StatusType.STARTED));
        assert(verifyCheckpointStatusTable(StatusType.IDLE, 0));
    }

    @Test
    public void initMultipleLeadersTest2() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime1, UUID.randomUUID());
        compactorLeaderServices1.setLeader(true);
        CompactorLeaderServices compactorLeaderServices2 = new CompactorLeaderServices(runtime2, UUID.randomUUID());
        compactorLeaderServices2.setLeader(true);

        ExecutorService scheduler = Executors.newFixedThreadPool(2);
        Future<Boolean> future1 = scheduler.submit(() -> compactorLeaderServices1.triggerCheckpointing());
        Future<Boolean> future2 = scheduler.submit(() -> compactorLeaderServices2.triggerCheckpointing());
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
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime1, UUID.randomUUID());
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.triggerCheckpointing();
        DistributedCompactor distributedCompactor1 =
                new DistributedCompactor(runtime1, cpRuntime1, null);
        DistributedCompactor distributedCompactor2 =
                new DistributedCompactor(runtime2, cpRuntime2, null);
        DistributedCompactor distributedCompactor3 =
                new DistributedCompactor(runtime3, cpRuntime3, null);

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
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime1, UUID.randomUUID());
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.triggerCheckpointing();

        DistributedCompactor distributedCompactor = new DistributedCompactor(runtime1, cpRuntime1, null);
        distributedCompactor.startCheckpointing();

        compactorLeaderServices1.finishCompactionCycle();

        assert(verifyManagerStatus(StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
    }

    @Test
    public void finishCompactionCycleFailureTest() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime1, UUID.randomUUID());
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.triggerCheckpointing();

        //Checkpointing not done
        compactorLeaderServices1.finishCompactionCycle();

        assert(verifyManagerStatus(StatusType.FAILED));
        assert(verifyCheckpointStatusTable(StatusType.IDLE, 0));
        assert(!verifyCheckpointTable());
    }

    private boolean pollForFinishCheckpointing() {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();
            txn.commit();
            log.info("managerStatus in test: {}", managerStatus.getStatus());
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
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime1, UUID.randomUUID());
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.triggerCheckpointing();

        DistributedCompactor distributedCompactor = new DistributedCompactor(runtime1, cpRuntime1, null);
        distributedCompactor.startCheckpointing();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> compactorLeaderServices1.validateLiveness(LIVENESS_TIMEOUT), 0,
                VALIDATE_LIVENESS_INTERVAL, TimeUnit.MILLISECONDS);
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
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime1, UUID.randomUUID());
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.triggerCheckpointing();
        compactorLeaderServices1.setLeader(false);

        DistributedCompactor distributedCompactor = new DistributedCompactor(runtime1, cpRuntime1, null);
        distributedCompactor.startCheckpointing();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> compactorLeaderServices1.validateLiveness(LIVENESS_TIMEOUT), 0,
                VALIDATE_LIVENESS_INTERVAL, TimeUnit.MILLISECONDS);

        try {
            TimeUnit.MILLISECONDS.sleep(LIVENESS_TIMEOUT);
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted, ", e);
        }

        assert(verifyManagerStatus(StatusType.STARTED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
        assert(!verifyCheckpointTable());
    }

    private static String STREAM_NAME = "streamNameA";

    @Test
    public void validateLivenessFailureTest() {
        CompactorLeaderServices compactorLeaderServices1 = new CompactorLeaderServices(runtime2, UUID.randomUUID());
        compactorLeaderServices1.setLeader(true);
        compactorLeaderServices1.triggerCheckpointing();

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
                VALIDATE_LIVENESS_INTERVAL, TimeUnit.MILLISECONDS);


        DistributedCompactor distributedCompactor = new DistributedCompactor(runtime1, cpRuntime1,
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
        assert(!verifyCheckpointTable());
    }
}
