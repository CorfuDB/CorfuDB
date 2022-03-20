package org.corfudb.runtime.checkpoint;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.proto.RpcCommon;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class DistributedCompactorTestTrial extends AbstractObjectTest {
//    private static final Logger log = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManager";
    private static final String CHECKPOINT_STATUS_TABLE_NAME = "CheckpointStatusTable";
    private static final StringKey COMPACTION_MANAGER_KEY =
            StringKey.newBuilder().setKey("CompactionManagerKey").build();
    private static final int N_THREADS = 3;

    public Table<StringKey, CheckpointingStatus, Message> openCompactionManagerTable(CorfuStore corfuStore) {
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

    public Table<TableName, CheckpointingStatus, Message> openCheckpointStatusTable(CorfuStore corfuStore) {
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

    private Table<StringKey, RpcCommon.UuidMsg, Message> openStreamA(CorfuStore corfuStore) {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    streamNameA,
                    StringKey.class,
                    RpcCommon.UuidMsg.class,
                    null,
                    TableOptions.fromProtoSchema(RpcCommon.UuidMsg.class));
        } catch (Exception e) {
            log.error("Exception while opening tables ", e);
        }
        return null;
    }

    private Table<StringKey, RpcCommon.UuidMsg, Message> openStreamB(CorfuStore corfuStore) {
        try {
            return corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    streamNameB,
                    StringKey.class,
                    RpcCommon.UuidMsg.class,
                    null,
                    TableOptions.fromProtoSchema(RpcCommon.UuidMsg.class));
        } catch (Exception e) {
            log.error("Exception while opening tables ", e);
        }
        return null;
    }

    private void addEntries(CorfuStore corfuStore) {
        TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE);
        txn.putRecord(openStreamA(corfuStore), StringKey.getDefaultInstance(), RpcCommon.UuidMsg.getDefaultInstance(), null);
        txn.putRecord(openStreamB(corfuStore), StringKey.getDefaultInstance(), RpcCommon.UuidMsg.getDefaultInstance(), null);
        txn.commit();
    }

    @Before
    public void instantiateMaps() {
        getDefaultRuntime();
    }

    final String streamNameA = "mystreamA";
    final String streamNameB = "mystreamB";
    final String author = "ckpointTest";

    private CorfuRuntime getNewRuntime() {
        return getNewRuntime(getDefaultNode()).connect();
    }

    @Test
    public void initTest() {
        CorfuRuntime corfuRuntime = getNewRuntime();
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        CorfuRuntime cpRuntime = getNewRuntime();
        DistributedCompactor distributedCompactor = new DistributedCompactor(corfuRuntime, cpRuntime, null, true);

        distributedCompactor.init();

        assert(checkManagerStatus(corfuStore, StatusType.STARTED));
        assert(checkCPStatusTable(corfuStore, StatusType.IDLE));
    }

    @Test
    public void startCheckpointingTest() {
        CorfuRuntime corfuRuntime = getNewRuntime();
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        CorfuRuntime cpRuntime = getNewRuntime();

        DistributedCompactor distributedCompactor = new DistributedCompactor(corfuRuntime, cpRuntime, null, true);

        openStreamA(corfuStore);
        openStreamB(corfuStore);
        distributedCompactor.init();
        distributedCompactor.startCheckpointing();

        assert(checkManagerStatus(corfuStore, StatusType.STARTED));
        assert(checkCPStatusTable(corfuStore, StatusType.COMPLETED));
    }

    @Test
    public void finishCompactionCycleTest() {
        CorfuRuntime corfuRuntime = getNewRuntime();
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        CorfuRuntime cpRuntime = getNewRuntime();


        DistributedCompactor distributedCompactor = new DistributedCompactor(corfuRuntime, cpRuntime, null, true);

        openStreamA(corfuStore);
        openStreamB(corfuStore);
        distributedCompactor.init();
        distributedCompactor.startCheckpointing();
        distributedCompactor.finishCompactionCycle();

        assert(checkManagerStatus(corfuStore, StatusType.COMPLETED));
    }

    @Test
    public void runCompactorTest() {
        CorfuRuntime corfuRuntime = getNewRuntime();
        CorfuRuntime cpRuntime = getNewRuntime();
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        DistributedCompactor distributedCompactor = new DistributedCompactor(corfuRuntime, cpRuntime, null, true);

        openStreamA(corfuStore);
        openStreamB(corfuStore);
        distributedCompactor.runCompactor();

        assert(checkManagerStatus(corfuStore, StatusType.COMPLETED));
        assert(checkCPStatusTable(corfuStore, StatusType.COMPLETED));
    }

    private void executeRunCompactor(CorfuRuntime corfuRuntime, boolean isLeader) {
        CorfuRuntime cpRuntime = getNewRuntime();
        DistributedCompactor distributedCompactor = new DistributedCompactor(corfuRuntime, cpRuntime,null, isLeader);
        distributedCompactor.runCompactor();
    }

    @Test
    public void threeNodeInitTest() {
        CorfuRuntime corfuRuntime = getNewRuntime();
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        openStreamA(corfuStore);
        openStreamB(corfuStore);

        ExecutorService executorService = Executors.newFixedThreadPool(N_THREADS);
        executorService.submit(() -> executeRunCompactor(getNewRuntime(), false));
        executorService.submit(() -> executeRunCompactor(corfuRuntime, true));
        executorService.submit(() -> executeRunCompactor(getNewRuntime(), false));

        try {
            final int TIMEOUT = 60;
                executorService.awaitTermination(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.info("Interrupted");
        }

        assert(checkManagerStatus(corfuStore, StatusType.COMPLETED));
        assert(checkCPStatusTable(corfuStore, StatusType.COMPLETED));
    }

    private boolean checkManagerStatus(CorfuStore corfuStore, StatusType targetStatus) {
        openCompactionManagerTable(corfuStore);
        TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE);
        CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(COMPACTION_MANAGER_TABLE_NAME,
                COMPACTION_MANAGER_KEY).getPayload();
        txn.close();
        System.out.println("ManagerStatus: " + managerStatus.getStatus());
        if (managerStatus.getStatus() != targetStatus) {
            return false;
        }
        return true;
    }

    private boolean checkCPStatusTable(CorfuStore corfuStore, StatusType targetStatus) {

        openCheckpointStatusTable(corfuStore);

        TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE);
        List<Object> tableNames = new ArrayList<Object>(txn.keySet(CHECKPOINT_STATUS_TABLE_NAME)
                .stream().collect(Collectors.toList()));
        boolean failed = false;
        for (Object table : tableNames) {
            CheckpointingStatus cpStatus = (CheckpointingStatus) txn.getRecord(CHECKPOINT_STATUS_TABLE_NAME,
                    (TableName) table).getPayload();
            System.out.println(((TableName) table).getTableName() + " : " + cpStatus.toString());
            if (cpStatus.getStatus() != targetStatus) {
                failed = true;
            }
        }
        txn.close();
        return !failed;
    }
}
