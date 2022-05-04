package org.corfudb.infrastructure;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.DistributedClientCheckpointer;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class CompactorServiceTest extends AbstractViewTest {

    ServerContext sc0;
    ServerContext sc1;
    ServerContext sc2;

    private static final int LIVENESS_TIMEOUT = 5000;
    private static final int WAIT_TO_KILL = 3000;
    private static final int COMPACTOR_SERVICE_INTERVAL = 10;

    private static final String CLIENT_NAME_PREFIX = "Client";

    private CorfuRuntime runtime0 = null;
    private CorfuRuntime runtime1 = null;
    private CorfuRuntime runtime2 = null;
    private CorfuRuntime cpRuntime0 = null;
    private CorfuRuntime cpRuntime1 = null;
    private CorfuRuntime cpRuntime2 = null;

    private CorfuStore corfuStore = null;

    private Set<String> clientIds = new HashSet<>();
    private boolean started_all = false;
    private Map<String, Table<StringKey, StringKey, Message>> openedStreams = new HashMap<>();
    private static String STREAM_KEY_PREFIX = "StreamKey";

    private final static Double logSizeLimitPercentageFull = 100.0;
    private final static Double logSizeLimitPercentageLow = 0.000002;

    /**
     * Generates and bootstraps a 3 node cluster in disk mode.
     * Shuts down the management servers of the 3 nodes.
     *
     * @return The generated layout.
     */
    private Layout setup3NodeCluster(Double logSizeLimitPercentage) {
        sc0 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_0))
                .setPort(SERVERS.PORT_0)
                .setMemory(false)
                .setCacheSizeHeapRatio("0.0")
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .setLogSizeLimitPercentage(Double.toString(logSizeLimitPercentage))
                .build();
        sc1 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_1))
                .setPort(SERVERS.PORT_1)
                .setMemory(false)
                .setCacheSizeHeapRatio("0.0")
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .setLogSizeLimitPercentage(Double.toString(logSizeLimitPercentage))
                .build();
        sc2 = new ServerContextBuilder()
                .setSingle(false)
                .setServerRouter(new TestServerRouter(SERVERS.PORT_2))
                .setPort(SERVERS.PORT_2)
                .setMemory(false)
                .setCacheSizeHeapRatio("0.0")
                .setLogPath(com.google.common.io.Files.createTempDir().getAbsolutePath())
                .setLogSizeLimitPercentage(Double.toString(logSizeLimitPercentage))
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

        return l;
    }

    public void testSetup(Double logSizeLimitPercentage) {
        Layout layout = setup3NodeCluster(logSizeLimitPercentage);
        runtime0 = getRuntime(layout).connect();
        runtime1 = getRuntime(layout).connect();
        runtime2 = getRuntime(layout).connect();
        runtime0.getParameters().setClientName(CLIENT_NAME_PREFIX + "0");
        runtime1.getParameters().setClientName(CLIENT_NAME_PREFIX + "1");
        runtime2.getParameters().setClientName(CLIENT_NAME_PREFIX + "2");

        cpRuntime0 = getRuntime(layout).connect();
        cpRuntime1 = getRuntime(layout).connect();
        cpRuntime2 = getRuntime(layout).connect();
        cpRuntime0.getParameters().setClientName(CLIENT_NAME_PREFIX + "_cp0");
        cpRuntime1.getParameters().setClientName(CLIENT_NAME_PREFIX + "_cp1");
        cpRuntime2.getParameters().setClientName(CLIENT_NAME_PREFIX + "_cp2");

        corfuStore = new CorfuStore(runtime0);

        System.out.println("testSetup completed");
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

    private Table<TableName, CorfuCompactorManagement.ActiveCPStreamMsg, Message> openActiveCheckpointsTable() {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.ACTIVE_CHECKPOINTS_TABLE_NAME,
                    TableName.class,
                    CorfuCompactorManagement.ActiveCPStreamMsg.class,
                    null,
                    TableOptions.fromProtoSchema(CorfuCompactorManagement.ActiveCPStreamMsg.class));
        } catch (Exception e) {
            log.error("Exception while opening tables ", e);
            return null;
        }
    }

    private Table<StringKey, RpcCommon.TokenMsg, Message> openCheckpointTable() {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT,
                    StringKey.class,
                    RpcCommon.TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));
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
            log.info("ManagerStatus: " + managerStatus);
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
                log.info("{} : {} clientId: {}", table.getTableName(), cpStatus.getStatus(), cpStatus.getClientName());
                clientIds.add(cpStatus.getClientName());
                if (cpStatus.getStatus() != targetStatus) {
                    failed++;
                }
            }
            return failed <= maxFailedTables;
        }
    }

    private boolean verifyCheckpointTable() {

        openCheckpointTable();

        RpcCommon.TokenMsg token;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            token = (RpcCommon.TokenMsg) txn.getRecord(DistributedCompactor.CHECKPOINT,
                    DistributedCompactor.CHECKPOINT_KEY).getPayload();
            txn.commit();
        }
        log.info("verify Token: {}", token == null ? "null" : token.toString());

        return (token != null);
    }

    private boolean pollForFinishCheckpointing() {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();
            txn.commit();
            if (managerStatus != null && (managerStatus.getStatus() == StatusType.COMPLETED
                    || managerStatus.getStatus() == StatusType.FAILED)) {
                log.info("done pollForFinishCp: {}", managerStatus.getStatus());
                return true;
            } else if (managerStatus != null && managerStatus.getStatus() == StatusType.STARTED_ALL) {
                started_all = true;
            }
        }
        return false;
    }

    private Table<StringKey, StringKey, Message> openStream(String streamName) {
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

    @Test
    public void singleServerTest() {
        testSetup(logSizeLimitPercentageFull);
        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime0);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy1 = new MockCompactionTriggerPolicy();

        CompactorService compactorService1 = new CompactorService(sc0, runtimeSingletonResource1,
                new InvokeCheckpointingMock(runtime0, cpRuntime0), mockCompactionTriggerPolicy1);
        compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService1.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy1.setShouldTrigger(true);

        try {
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted, ", e);
        }

        assert(verifyManagerStatus(StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
    }

    @Test
    public void multipleServerTest() {
        testSetup(logSizeLimitPercentageFull);
        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime0);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy1 = new MockCompactionTriggerPolicy();
        SingletonResource<CorfuRuntime> runtimeSingletonResource2 = SingletonResource.withInitial(() -> runtime1);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy2 = new MockCompactionTriggerPolicy();

        CompactorService compactorService1 = new CompactorService(sc0, runtimeSingletonResource1,
                new InvokeCheckpointingMock(runtime0, cpRuntime0), mockCompactionTriggerPolicy1);
        compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService1.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy1.setShouldTrigger(true);

        CompactorService compactorService2 = new CompactorService(sc1, runtimeSingletonResource2,
                new InvokeCheckpointingMock(runtime1, cpRuntime1), mockCompactionTriggerPolicy2);
        compactorService2.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService2.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy2.setShouldTrigger(false);

        try {
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted, ", e);
        }

        assert(verifyManagerStatus(StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
    }

    @Test
    public void leaderFailureTest() {
        testSetup(logSizeLimitPercentageFull);
        SingletonResource<CorfuRuntime> runtimeSingletonResource0 = SingletonResource.withInitial(() -> runtime0);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy0 = new MockCompactionTriggerPolicy();
        CompactorService compactorService0 = new CompactorService(sc0, runtimeSingletonResource0,
                new InvokeCheckpointingMock(runtime0, cpRuntime0), mockCompactionTriggerPolicy0);
        compactorService0.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService0.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy0.setShouldTrigger(true);

        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime1);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy1 = new MockCompactionTriggerPolicy();
        CompactorService compactorService1 = new CompactorService(sc1, runtimeSingletonResource1,
                new InvokeCheckpointingMock(runtime1, cpRuntime1), mockCompactionTriggerPolicy1);
        compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService1.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy1.setShouldTrigger(false);

        SingletonResource<CorfuRuntime> runtimeSingletonResource2 = SingletonResource.withInitial(() -> runtime2);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy2 = new MockCompactionTriggerPolicy();
        CompactorService compactorService2 = new CompactorService(sc2, runtimeSingletonResource2,
                new InvokeCheckpointingMock(runtime2, cpRuntime2), mockCompactionTriggerPolicy2);
        compactorService2.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService2.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy2.setShouldTrigger(false);

        try {
            TimeUnit.MILLISECONDS.sleep(WAIT_TO_KILL);
            shutdownServer(SERVERS.PORT_0);
            compactorService0.shutdown();

            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (Exception e) {
            log.warn("Exception: ", e);
        }

        assert(verifyManagerStatus(StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
    }

    @Test
    public void nonLeaderFailureTest() {
        testSetup(logSizeLimitPercentageFull);
        SingletonResource<CorfuRuntime> runtimeSingletonResource0 = SingletonResource.withInitial(() -> runtime0);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy0 = new MockCompactionTriggerPolicy();
        CompactorService compactorService0 = new CompactorService(sc0, runtimeSingletonResource0,
                new InvokeCheckpointingMock(runtime0, cpRuntime0), mockCompactionTriggerPolicy0);
        compactorService0.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService0.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy0.setShouldTrigger(true);

        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime1);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy1 = new MockCompactionTriggerPolicy();
        CompactorService compactorService1 = new CompactorService(sc1, runtimeSingletonResource1,
                new InvokeCheckpointingMock(runtime1, cpRuntime1), mockCompactionTriggerPolicy1);
        compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService1.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy1.setShouldTrigger(false);

        SingletonResource<CorfuRuntime> runtimeSingletonResource2 = SingletonResource.withInitial(() -> runtime2);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy2 = new MockCompactionTriggerPolicy();
        CompactorService compactorService2 = new CompactorService(sc2, runtimeSingletonResource2,
                new InvokeCheckpointingMock(runtime2, cpRuntime2), mockCompactionTriggerPolicy2);
        compactorService2.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService2.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy2.setShouldTrigger(false);

        try {
            TimeUnit.MILLISECONDS.sleep(WAIT_TO_KILL);
            shutdownServer(SERVERS.PORT_1);
            compactorService1.shutdown();

            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (Exception e) {
            log.warn("Exception: ", e);
        }

        assert(verifyManagerStatus(StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
    }

    @Test
    public void clientsCheckpointing() {
        testSetup(logSizeLimitPercentageFull);
        runtime2.getParameters().setCheckpointTriggerFreqMillis(COMPACTOR_SERVICE_INTERVAL);
        DistributedClientCheckpointer distributedClientCheckpointer0 = new DistributedClientCheckpointer(runtime2);

        runtime1.getParameters().setCheckpointTriggerFreqMillis(COMPACTOR_SERVICE_INTERVAL);
        DistributedClientCheckpointer distributedClientCheckpointer1 = new DistributedClientCheckpointer(runtime1);

        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime0);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy1 = new MockCompactionTriggerPolicy();

        CompactorService compactorService1 = new CompactorService(sc0, runtimeSingletonResource1,
                new InvokeCheckpointingMock(runtime0, cpRuntime0), mockCompactionTriggerPolicy1);
        compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService1.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy1.setShouldTrigger(true);

        try {
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
                distributedClientCheckpointer0.shutdown();
                distributedClientCheckpointer1.shutdown();
            }
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted, ", e);
        }

        assert(verifyManagerStatus(StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
        assert(!started_all);
    }

    @Test
    public void serverCheckpointsUnopenedTables() {
        testSetup(logSizeLimitPercentageFull);
        openStream(STREAM_KEY_PREFIX);

        runtime2.getParameters().setCheckpointTriggerFreqMillis(COMPACTOR_SERVICE_INTERVAL);
        DistributedClientCheckpointer distributedClientCheckpointer = new DistributedClientCheckpointer(runtime2);

        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime0);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy1 = new MockCompactionTriggerPolicy();

        CompactorService compactorService1 = new CompactorService(sc0, runtimeSingletonResource1,
                new InvokeCheckpointingMock(runtime0, cpRuntime0), mockCompactionTriggerPolicy1);
        compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService1.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy1.setShouldTrigger(true);

        try {
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
                distributedClientCheckpointer.shutdown();
            }
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted, ", e);
        }

        assert(verifyManagerStatus(StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
        //asserts that the server invoked default checkpointing
        assert(started_all);
    }

    @Test
    public void checkpointFailureTest() {
        testSetup(logSizeLimitPercentageFull);
        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime0);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy1 = new MockCompactionTriggerPolicy();

        CompactorService compactorService1 = new CompactorService(sc0, runtimeSingletonResource1,
                new InvokeCheckpointingMock(runtime0, cpRuntime0), mockCompactionTriggerPolicy1);
        compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService1.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy1.setShouldTrigger(true);

        try {
            TimeUnit.MILLISECONDS.sleep(LIVENESS_TIMEOUT);
            Table<TableName, CorfuCompactorManagement.ActiveCPStreamMsg, Message> activeCheckpointTable = openActiveCheckpointsTable();
            Table<TableName, CheckpointingStatus, Message> checkpointStatusTable = openCheckpointStatusTable();
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                TableName table = TableName.newBuilder().setNamespace(CORFU_SYSTEM_NAMESPACE).setTableName(STREAM_KEY_PREFIX).build();
                //Adding a table with STARTED value - making it look like someone started and died while checkpointing
                txn.putRecord(checkpointStatusTable, table,
                        CheckpointingStatus.newBuilder().setStatusValue(StatusType.STARTED_VALUE).build(), null);
                txn.putRecord(activeCheckpointTable,
                        table,
                        CorfuCompactorManagement.ActiveCPStreamMsg.getDefaultInstance(),
                        null);
                txn.commit();
            }

            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted, ", e);
        }

        assert(verifyManagerStatus(StatusType.FAILED));
        assert(verifyCheckpointStatusTable(StatusType.IDLE, 1));
        //asserts that the server invoked checkpointing
        assert(!started_all);
    }

    @Test
    public void quotaExceededTest() {
        testSetup(logSizeLimitPercentageLow);
        SingletonResource<CorfuRuntime> runtimeSingletonResource1 = SingletonResource.withInitial(() -> runtime0);
        MockCompactionTriggerPolicy mockCompactionTriggerPolicy1 = new MockCompactionTriggerPolicy();

        CompactorService compactorService1 = new CompactorService(sc0, runtimeSingletonResource1,
                new InvokeCheckpointingMock(runtime0, cpRuntime0), mockCompactionTriggerPolicy1);
        compactorService1.start(Duration.ofMillis(COMPACTOR_SERVICE_INTERVAL));
        compactorService1.setLivenessTimeout(LIVENESS_TIMEOUT);
        mockCompactionTriggerPolicy1.setShouldTrigger(true);

        try {
            while (!pollForFinishCheckpointing()) {
                TimeUnit.MILLISECONDS.sleep(COMPACTOR_SERVICE_INTERVAL);
            }
        } catch (InterruptedException e) {
            log.warn("Sleep interrupted, ", e);
        }

        assert(verifyManagerStatus(StatusType.COMPLETED));
        assert(verifyCheckpointStatusTable(StatusType.COMPLETED, 0));
        assert(verifyCheckpointTable());
    }
}
