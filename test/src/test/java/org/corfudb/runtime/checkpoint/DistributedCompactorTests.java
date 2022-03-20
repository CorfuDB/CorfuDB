package org.corfudb.runtime.checkpoint;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class DistributedCompactorTests extends AbstractViewTest {

    private static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManager";
    private static final String CHECKPOINT_STATUS_TABLE_NAME = "CheckpointStatusTable";
    public static final String CHECKPOINT = "checkpoint";

    private static final StringKey COMPACTION_MANAGER_KEY =
            StringKey.newBuilder().setKey("CompactionManagerKey").build();
    private static final StringKey CHECKPOINT_KEY = StringKey.newBuilder().setKey("minCheckpointToken").build();

    private static final int N_THREADS = 3;

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
                    COMPACTION_MANAGER_TABLE_NAME,
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
                    CHECKPOINT_STATUS_TABLE_NAME,
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
                    CHECKPOINT,
                    StringKey.class,
                    TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(TokenMsg.class));
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

    private boolean executeRunCompactor(CorfuRuntime corfuRuntime, CorfuRuntime cpRuntime, boolean isLeader) {
        try {
            System.out.println("runtime clientId: "+corfuRuntime.getParameters().getClientId());
            DistributedCompactor distributedCompactor = new DistributedCompactor(
                    corfuRuntime, cpRuntime, null, isLeader);
            distributedCompactor.runCompactor();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private boolean verifyManagerStatus(CheckpointingStatus.StatusType targetStatus) {
        openCompactionManagerTable();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(COMPACTION_MANAGER_TABLE_NAME,
                    COMPACTION_MANAGER_KEY).getPayload();
            System.out.println("ManagerStatus: " + managerStatus.getStatus());
            if (managerStatus.getStatus() == targetStatus) {
                System.out.println("verifyManagerStatus: returning true");
                return true;
            }
        }
        return true;
    }

    private boolean verifyCheckpointStatusTable(CheckpointingStatus.StatusType targetStatus, int maxFailedTables) {

        Table<TableName, CheckpointingStatus, Message> cpStatusTable = openCheckpointStatusTable();

        int failed = 0;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            List<TableName> tableNames = new ArrayList<>(txn.keySet(cpStatusTable)
                    .stream().collect(Collectors.toList()));
            for (TableName table : tableNames) {
                CheckpointingStatus cpStatus = (CheckpointingStatus) txn.getRecord(CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                System.out.println(table.getTableName() +
                        " : " + cpStatus.getStatus() + " clientId: " + cpStatus.getClientId());
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
            token = (TokenMsg) txn.getRecord(CHECKPOINT, CHECKPOINT_KEY).getPayload();
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
        DistributedCompactor distributedCompactor1 =
                new DistributedCompactor(runtime1, cpRuntime1, null, true);
        DistributedCompactor distributedCompactor2 =
                new DistributedCompactor(runtime2, cpRuntime2, null, false);
        DistributedCompactor distributedCompactor3 =
                new DistributedCompactor(runtime3, cpRuntime3, null, true);

        assert(distributedCompactor1.init());
        assert(distributedCompactor2.init());
        assert(!distributedCompactor3.init());

        assert(verifyManagerStatus(CheckpointingStatus.StatusType.STARTED));
        assert(verifyCheckpointStatusTable(CheckpointingStatus.StatusType.IDLE, 0));
    }

    @Test
    public void startCheckpointingTest() {
        DistributedCompactor distributedCompactor1 =
                new DistributedCompactor(runtime1, cpRuntime1, null, true);
        DistributedCompactor distributedCompactor2 =
                new DistributedCompactor(runtime2, cpRuntime2, null, false);
        DistributedCompactor distributedCompactor3 =
                new DistributedCompactor(runtime3, cpRuntime3, null, false);

        distributedCompactor1.init();
        int count1 = distributedCompactor1.startCheckpointing();
        int count2 = distributedCompactor2.startCheckpointing();
        int count3 = distributedCompactor3.startCheckpointing();
        int total = count1 + count2 + count3;

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            //This assert ensures all tables are checkpointed by only one of the clients
            Assert.assertEquals(txn.count(CHECKPOINT_STATUS_TABLE_NAME), total);
            txn.commit();
        }
        assert(verifyManagerStatus(CheckpointingStatus.StatusType.STARTED));
        assert(verifyCheckpointStatusTable(CheckpointingStatus.StatusType.COMPLETED, 0));
    }

    @Test
    public void finishCompactionCycleTest() {
        DistributedCompactor distributedCompactor1 =
                new DistributedCompactor(runtime1, cpRuntime1, null, true);

        distributedCompactor1.init();
        distributedCompactor1.finishCompactionCycle();

        assert(verifyManagerStatus(CheckpointingStatus.StatusType.FAILED));
        assert(verifyCheckpointStatusTable(CheckpointingStatus.StatusType.IDLE, 0));
    }

    /**
     * Happy path. 3 Compaction clients checkpoint in parallel with one of them acting as the leader
     */
    @Test
    public void testDistributorCompactor() {
        for (int i = 0; i < N_THREADS*2; i++) {
            openStream(corfuStore, STREAM_NAME_PREFIX + i);
            populateStream(STREAM_NAME_PREFIX + i, NUM_RECORDS);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(N_THREADS);
        executorService.submit(() -> executeRunCompactor(runtime1, cpRuntime1, false));
        executorService.submit(() -> executeRunCompactor(runtime2, cpRuntime2,false));
        executorService.submit(() -> executeRunCompactor(runtime3, cpRuntime3, true));

        try {
            final int TIMEOUT = 45;
            executorService.awaitTermination(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.info("Interrupted");
        }

        assert(verifyManagerStatus(CheckpointingStatus.StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(CheckpointingStatus.StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
    }

    /**
     * One of the non-leader Compaction clients fails abruptly.
     */
    @Test
    public void testNonLeaderFails() {
        for (int i = 0; i < N_THREADS*2; i++) {
            openStream(corfuStore, STREAM_NAME_PREFIX + i);
            populateStream(STREAM_NAME_PREFIX + i, NUM_RECORDS);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(N_THREADS);
        Future<Boolean> future1 = executorService.submit(() -> executeRunCompactor(runtime1, cpRuntime1, false));
        executorService.submit(() -> executeRunCompactor(runtime2, cpRuntime2,false));
        executorService.submit(() -> executeRunCompactor(runtime3, cpRuntime3, true));

        try {
            final int WAIT = 236;
            TimeUnit.MILLISECONDS.sleep(WAIT);
            future1.cancel(true);
            executorService.awaitTermination(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.info("Interrupted");
        }

        assert(verifyManagerStatus(CheckpointingStatus.StatusType.FAILED));
        assert(verifyCheckpointStatusTable(CheckpointingStatus.StatusType.COMPLETED, 1));
        assert(verifyCheckpointTable());
    }

    /**
     * The leader Compaction clients fails abruptly.
     */
    @Test
    public void testLeaderFails() {
        for (int i = 0; i < N_THREADS*2; i++) {
            openStream(corfuStore, STREAM_NAME_PREFIX + i);
            populateStream(STREAM_NAME_PREFIX + i, NUM_RECORDS);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(N_THREADS);
        executorService.submit(() -> executeRunCompactor(runtime1, cpRuntime1, false));
        executorService.submit(() -> executeRunCompactor(runtime2, cpRuntime2,false));
        Future<Boolean> future3 = executorService.submit(() -> executeRunCompactor(runtime3, cpRuntime3, true));

        try {
            final int WAIT = 236;
            TimeUnit.MILLISECONDS.sleep(WAIT);
            future3.cancel(true);
            executorService.awaitTermination(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.info("Interrupted");
        }

        assert(verifyManagerStatus(CheckpointingStatus.StatusType.STARTED));
        assert(verifyCheckpointStatusTable(CheckpointingStatus.StatusType.COMPLETED, 1));
        assert(!verifyCheckpointTable());
    }

    public class StragglerThread extends Thread {
        CorfuRuntime runtime;
        CorfuRuntime cpRuntime;
        Boolean isLeader;
        public StragglerThread (CorfuRuntime runtime, CorfuRuntime cpRuntime, boolean isLeader) {
            this.runtime = runtime;
            this.cpRuntime = cpRuntime;
            this.isLeader = isLeader;
        }
        public void run() {
            executeRunCompactor(runtime, cpRuntime, isLeader);
        }
    }

    /**
     * Non-leader node is a straggler
     */
//    @Test
    //Incomplete
    public void testNonLeaderStraggler() {
        for (int i = 0; i < N_THREADS*2; i++) {
            openStream(corfuStore, STREAM_NAME_PREFIX + i);
            populateStream(STREAM_NAME_PREFIX + i, NUM_RECORDS);
        }

        StragglerThread thread1 = new StragglerThread(runtime1, cpRuntime1, false);
        StragglerThread thread2 = new StragglerThread(runtime2, cpRuntime2, false);
        StragglerThread thread3 = new StragglerThread(runtime3, cpRuntime3, true);
        thread1.start();
        thread2.start();
        thread3.start();


        try {
            final int INITIAL_WAIT = 100;
            final int WAIT = 10;
            TimeUnit.MILLISECONDS.sleep(INITIAL_WAIT);
            while (thread2.isAlive()) {
                thread2.sleep(INITIAL_WAIT);
                System.out.println("thread2 is sleeping");
                TimeUnit.MILLISECONDS.sleep(WAIT);
            }
        } catch (InterruptedException e) {
            log.info("Interrupted");
        }

        verifyManagerStatus(CheckpointingStatus.StatusType.COMPLETED);
        verifyCheckpointStatusTable(CheckpointingStatus.StatusType.COMPLETED, 0);
        log.info("verifyCheckpointTable: {}", verifyCheckpointTable());
    }

    /**
     * Split brain scenario
     */
    @Test
    public void testSplitBrain1() {
        for (int i = 0; i < N_THREADS*2; i++) {
            openStream(corfuStore, STREAM_NAME_PREFIX + i);
            populateStream(STREAM_NAME_PREFIX + i, NUM_RECORDS);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(N_THREADS);
        executorService.submit(() -> executeRunCompactor(runtime1, cpRuntime1, false));
        executorService.submit(() -> executeRunCompactor(runtime2, cpRuntime2,true));
        executorService.submit(() -> executeRunCompactor(runtime3, cpRuntime3, true));

        try {
            executorService.awaitTermination(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.info("Interrupted");
        }

        assert(verifyManagerStatus(CheckpointingStatus.StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(CheckpointingStatus.StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
    }
}
