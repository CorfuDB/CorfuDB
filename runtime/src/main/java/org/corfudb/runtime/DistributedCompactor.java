package org.corfudb.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.*;

/**
 * DistributedCompactor class performs checkpointing of tables in multiple clients in parallel.
 * If the DistributedCompactor is triggered by the server, all opened and unopened tables are checkpointed. Otherwise,
 * only opened tables are checkpointed.
 * To checkpoint a table -
 * 1. Check if the table is locked for checkpointing by any other client ie. check if the Status of the table is IDLE
 * 2. Acquire lock to checkpoint the table, ie. mark table's status as STARTED if no other client already did
 * 3. Perform checkpointing for the table's stream
 * 4. Release lock to checkpoint the table ie. mark table's status as FAILED/COMPLETED
 */

@Slf4j
public class DistributedCompactor {
    private final CorfuRuntime runtime;
    private final CorfuRuntime cpRuntime;
    private final String persistedCacheRoot;
    private final boolean isClient;

    private KeyDynamicProtobufSerializer keyDynamicProtobufSerializer;

    private final String clientName;

    public static final long CONN_RETRY_DELAY_MILLISEC = 500;
    public static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManager";
    public static final String CHECKPOINT_STATUS_TABLE_NAME = "CheckpointStatusTable";
    public static final String ACTIVE_CHECKPOINTS_TABLE_NAME = "ActiveCheckpoints";

    public static final String NODE_TOKEN = "node-token";
    public static final String CHECKPOINT = "checkpoint";

    //TODO: have a special table list if required - like ALL_OPENED_CLUSTERING_STREAMS

    public static final StringKey COMPACTION_MANAGER_KEY = StringKey.newBuilder().setKey("CompactionManagerKey").build();
    public static final StringKey CHECKPOINT_KEY = StringKey.newBuilder().setKey("minCheckpointToken").build();

    public static class CorfuTableNamePair {
        public TableName tableName;
        public CorfuTable corfuTable;
        public CorfuTableNamePair(TableName tableName, CorfuTable corfuTable) {
            this.tableName = tableName;
            this.corfuTable = corfuTable;
        }
    }

    private CorfuStore corfuStore = null;
    private Table<StringKey, CheckpointingStatus, Message> compactionManagerTable = null;
    private Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable = null;
    private Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointsTable = null;
    private Table<StringKey, RpcCommon.TokenMsg, Message> checkpointTable = null;

    public DistributedCompactor(CorfuRuntime corfuRuntime, CorfuRuntime cpRuntime, String persistedCacheRoot) {
        this.runtime = corfuRuntime;
        this.cpRuntime = cpRuntime;
        this.persistedCacheRoot = persistedCacheRoot;
        this.isClient = false;
        this.clientName = corfuRuntime.getParameters().getClientName();
    }

    public DistributedCompactor(CorfuRuntime corfuRuntime) {
        this.runtime = corfuRuntime;
        this.cpRuntime = null;
        this.persistedCacheRoot = null;
        this.isClient = true;
        this.clientName = corfuRuntime.getParameters().getClientName();
    }

    private void openCheckpointingMetadataTables() {
        try {
            if (this.corfuStore != null) { // only run this method once
                return;
            }
            this.corfuStore = new CorfuStore(this.runtime);
            log.debug("Opening all the checkpoint metadata tables");
            this.compactionManagerTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    StringKey.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));

            this.checkpointingStatusTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT_STATUS_TABLE_NAME,
                    TableName.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));

            this.activeCheckpointsTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.ACTIVE_CHECKPOINTS_TABLE_NAME,
                    TableName.class,
                    ActiveCPStreamMsg.class,
                    null,
                    TableOptions.fromProtoSchema(ActiveCPStreamMsg.class));

            this.checkpointTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT,
                    StringKey.class,
                    RpcCommon.TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));
        } catch (Exception e) {
            log.error("Caught an exception while opening checkpoint management tables ", e);
        }
    }

    private boolean checkCompactionManagerStartTrigger() {
        // TODO: This is necessary here to stop checkpointing after it has started?
        // if (isCheckpointFrozen(corfuStore, this.checkpointFreezeTable)) {return;}
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            final CheckpointingStatus managerStatus = txn.getRecord(
                    compactionManagerTable, DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();
            txn.commit();
            if (managerStatus == null ||
                    (managerStatus.getStatus() != StatusType.STARTED &&
                    managerStatus.getStatus() != StatusType.STARTED_ALL)) {
                return false;
            }
            log.warn("ManagerStatus: {}", (managerStatus == null ? "null" : managerStatus.getStatus()));
        } catch (Exception e) {
            log.error("Unable to acquire CompactionManager status, {}, {}", e, e.getStackTrace());
            return false;
        }
        return true;
    }

    /**
     * An entry for all the clients participating in checkpointing
     * Tries to checkpoint tables that are marked as IDLE
     * @return count - the number of tables checkpointed by the client
     */
    public int startCheckpointing() {
        long startCp = System.currentTimeMillis();
        int count = 0;
        log.trace("Starting Checkpointing in-memory tables..");
        openCheckpointingMetadataTables();
        count = checkpointOpenedTables();

        if (count <= 0) {
            log.trace("Opened tables Checkpoint not done by client: {}", clientName);
            if (count < 0) {
                return count;
            }
        }

        if (!isClient) {
            log.info("{} in-memory tables checkpointed using protobuf serializer. Checkpointing remaining.", count);
            count += checkpointUnopenedTables();
        }

        if (count > 0) {
            long cpDuration = System.currentTimeMillis() - startCp;
            log.info("Client {} took {} ms to checkpoint {} tables", clientName, cpDuration, count);

            MicroMeterUtils.time(Duration.ofMillis(cpDuration), "checkpoint.total.client_timer",
                    "clientName", clientName);
            MicroMeterUtils.measure(count, "checkpoint.num_tables." + clientName);
        }

        return count;
    }

    private int checkpointUnopenedTables() {
        int count = 0;

        keyDynamicProtobufSerializer = new KeyDynamicProtobufSerializer(cpRuntime);
        cpRuntime.getSerializers().registerSerializer(keyDynamicProtobufSerializer);

        List<TableName> tableNames = getAllTablesToCheckpoint();
        for (TableName tableName : tableNames) {
            final CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord> corfuTable =
                    openTable(tableName, keyDynamicProtobufSerializer, cpRuntime);
            final int successfulCheckpoints = tryCheckpointTable(tableName, corfuTable);
            if (successfulCheckpoints < 0) {
                // finishCompactionCycle will mark it as failed
                MicroMeterUtils.measure(count, "compaction.num_tables." + clientName);
                return count;
            }
            count += successfulCheckpoints; // 0 means lock failed so don't tick up count
        }

        return count;
    }

    private List<TableName> getAllTablesToCheckpoint() {
        List<TableName> tablesToCheckpoint = null;
        final int maxRetry = 5;
        for (int retry = 0; retry < maxRetry; retry++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                tablesToCheckpoint = new ArrayList<>(txn.keySet(checkpointingStatusTable)
                        .stream().collect(Collectors.toList()));
                txn.commit();
            } catch (RuntimeException re) {
                if (!isCriticalRuntimeException(re, retry, maxRetry)) {
                    return null;
                }
            } catch (Throwable t) {
                log.error("getAllTablesToCheckpoint: encountered unexpected exception", t);
            }
        }
        return tablesToCheckpoint;
    }

    private ILivenessUpdater livenessUpdater = new ILivenessUpdater() {
        private ScheduledExecutorService executorService;

        private final static int updateInterval = 15000;

        @Override
        public void updateLiveness(TableName tableName) {
            // update validity counter every 15s
            executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleWithFixedDelay(() -> {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    ActiveCPStreamMsg currentStatus =
                            txn.getRecord(activeCheckpointsTable, tableName).getPayload();
                    ActiveCPStreamMsg newStatus = ActiveCPStreamMsg.newBuilder()
                                    .setSyncHeartbeat(currentStatus.getSyncHeartbeat() + 1)
                                    .setIsClientTriggered(currentStatus.getIsClientTriggered())
                                    .build();
                    txn.putRecord(activeCheckpointsTable, tableName, newStatus, null);
                    txn.commit();
                } catch (Exception e) {
                    log.error("Unable to update liveness for table: {}", tableName);
                }
            }, updateInterval/2, updateInterval, TimeUnit.MILLISECONDS);
        }

        @Override
        public void notifyOnSyncComplete() {
            executorService.shutdownNow();
        }
    };

    public <K, V> CheckpointingStatus appendCheckpoint(CorfuTable<K, V> corfuTable, TableName tableName, CorfuRuntime rt) {
        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();
        mcw.addMap(corfuTable);
        long tableCkptStartTime = System.currentTimeMillis();
        log.info("{} Starting checkpoint: {}${}", clientName,
                tableName.getNamespace(), tableName.getTableName());

        livenessUpdater.updateLiveness(tableName);
        CheckpointingStatus returnStatus = null;
        try {
            Token trimPoint = mcw.appendCheckpoints(rt, "checkpointer", livenessUpdater);
            returnStatus = CheckpointingStatus.newBuilder()
                    .setStatus(CheckpointingStatus.StatusType.COMPLETED)
                    .setClientName(this.clientName)
                    .setTableSize(corfuTable.size())
                    .setTimeTaken(System.currentTimeMillis() - tableCkptStartTime)
                    .build();
        } catch (Exception e) {
            log.error("Unable to checkpoint table: {}, e: {}", tableName, e);
            returnStatus = CheckpointingStatus.newBuilder()
                    .setStatus(CheckpointingStatus.StatusType.FAILED)
                    .setClientName(this.clientName)
                    .setTableSize(corfuTable.size())
                    .setTimeTaken(System.currentTimeMillis() - tableCkptStartTime)
                    .build();
        }
        livenessUpdater.notifyOnSyncComplete();
        return returnStatus;
    }

    private CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord> openTable(TableName tableName,
                                                                            ISerializer serializer,
                                                                            CorfuRuntime rt) {
        log.info("Opening table {} in namespace {}", tableName.getTableName(), tableName.getNamespace());
        SMRObject.Builder<CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord>> corfuTableBuilder =
                rt.getObjectsView().build()
                        .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord>>() {
                        })
                        .setStreamName(getFullyQualifiedTableName(tableName.getNamespace(), tableName.getTableName()))
                        .setSerializer(serializer)
                        .addOpenOption(ObjectOpenOption.NO_CACHE);
        if (persistedCacheRoot == null || persistedCacheRoot == "") {
            log.warn("Table {}::{} should be opened in disk-mode, but disk cache path is invalid",
                    tableName.getNamespace(), tableName.getTableName());
        } else {
            final String persistentCacheDirName = String.format("compactor_%s_%s",
                    tableName.getNamespace(), tableName.getTableName());
            final Path persistedCacheLocation = Paths.get(persistedCacheRoot).resolve(persistentCacheDirName);
            final Supplier<StreamingMap<CorfuDynamicKey, OpaqueCorfuDynamicRecord>> mapSupplier =
                    () -> new PersistedStreamingMap<>(
                            persistedCacheLocation, PersistedStreamingMap.getPersistedStreamingMapOptions(),
                            serializer, rt);
            corfuTableBuilder.setArguments(mapSupplier, ICorfuVersionPolicy.MONOTONIC);
        }
        return corfuTableBuilder.open();
    }

    /**
     * Checkpoint all tables already opened by the current JVM to save on reads & memory.
     * @return positive count on success and negative value on failure
     */
    public synchronized int checkpointOpenedTables() {
        if (!checkCompactionManagerStartTrigger()) {
            log.trace("Checkpoint hasn't started");
            return 0; // Orchestrator says checkpointing is either not needed or done.
        }
        log.trace("Checkpointing opened tables");
        int count = 0;
        for (CorfuTableNamePair openedTable :
                this.runtime.getTableRegistry().getAllOpenTablesForCheckpointing()) {
            final int isSuccess = tryCheckpointTable(openedTable.tableName, openedTable.corfuTable);
            if (isSuccess < 0) {
                log.warn("Stopping checkpointing after failure in {}${}",
                        openedTable.tableName.getNamespace(), openedTable.tableName.getTableName());
                return -count; // Stop checkpointing other tables on first failure
            }
            count += isSuccess; // 0 means lock failed so don't uptick count of checkpointed tables
        }
        return count; // All open tables have been successfully checkpointed
    }

    /**
     * Distributed Checkpointing involves 3 steps:
     * 1. Acquire distributed lock on the table to be checkpointed using transactions.
     * 2. Attempt to checkpoint the table, retry on retryable errors like WrongEpochException.
     * 3. If successful, unlock the table. if unsuccessful mark the checkpoint as failed.
     * @param corfuTable - the locally opened Table instance to be checkpointed
     * @return 1 on success, 0 if lock failed and negative value on error
     */
    private int tryCheckpointTable(TableName tableName, CorfuTable corfuTable) {
        if (!tryLockMyTableToCheckpoint(tableName)) {
            return 0; // Failure to get a lock is treated as success
        }

        CheckpointingStatus checkpointStatus = tryAppendCheckpoint(tableName, corfuTable);

        return unlockMyCheckpointTable(tableName, checkpointStatus);
    }

    /**
     * @param tableName - protobuf name of the table used as key for the granular lock
     * @return true if the table can be checkpointed by me
     *          false if lock acquisition fails due to race or a different error
     */
    private boolean tryLockMyTableToCheckpoint(TableName tableName) {
        final int maxRetries = 5;
        for (int retry = 0; retry < maxRetries; retry++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                final CorfuStoreEntry<TableName, CheckpointingStatus, Message> tableToChkpt =
                        txn.getRecord(checkpointingStatusTable, tableName);
                if (tableToChkpt.getPayload().getStatus() == StatusType.IDLE) {
                    txn.putRecord(checkpointingStatusTable,
                            tableName,
                            CheckpointingStatus.newBuilder()
                                    .setStatus(CheckpointingStatus.StatusType.STARTED)
                                    .setClientName(clientName)
                                    .build(),
                            null);
                    txn.putRecord(activeCheckpointsTable, tableName,
                            ActiveCPStreamMsg.newBuilder()
                                    .setIsClientTriggered(isClient) // This will stall server side compaction!
                                    .build(),
                            null);
                } else { // This table is already being checkpointed by someone else
                    txn.commit();
                    return false;
                }
                txn.commit();
                return true; // Lock successfully acquired!
            } catch (TransactionAbortedException e) {
                if (e.getAbortCause() == AbortCause.CONFLICT) {
                    log.info("My opened table {}${} is being checkpointed by someone else",
                            tableName.getNamespace(), tableName.getTableName());
                    return false;
                }
            } catch (RuntimeException re) { // TODO: return a different error code maybe?
                if (isCriticalRuntimeException(re, retry, maxRetries)) {
                    return false; // stop on non-retryable exceptions
                }
            } catch (Throwable t) {
                log.error("clientChpt-tryLock: encountered unexpected exception, {}", t.getMessage());
                log.error("StackTrace: {}, Cause: {}", t.getStackTrace(), t.getCause());
            }
        }
        return false;
    }

    /**
     * This is the routine where the actual checkpointing is invoked
     * @param tableName - protobuf name of the table to be checkpointed
     * @param corfuTable - the table to be checkpointed
     * @return the token returned from the checkpointing, null if failure happens
     */
    private CheckpointingStatus tryAppendCheckpoint(TableName tableName, CorfuTable corfuTable) {
        final int maxRetries = 5;
        CheckpointingStatus failedStatus = CheckpointingStatus.newBuilder()
                .setStatus(CheckpointingStatus.StatusType.FAILED)
                .setClientName(clientName)
                .setTableSize(corfuTable.size())
                .build();
        for (int retry = 0; retry < maxRetries; retry++) {
            try {
                return appendCheckpoint(corfuTable, tableName, runtime);
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, maxRetries)) {
                    return failedStatus; // stop on non-retryable exceptions
                }
            } catch (Throwable t) {
                log.error("clientCheckpointer: encountered unexpected exception", t);
                log.error("StackTrace: {}", t.getStackTrace());
            }
        }
        return failedStatus;
    }

    /**
     * Mark the checkpointed table as either done or failed based on endToken
     * @param tableName - protobuf name of the table just checkpointed
     * @param checkpointStatus - status of checkpoint with all the info
     * @return - 1 on success, 0 or negative value on failure
     */
    private int unlockMyCheckpointTable(TableName tableName, CheckpointingStatus checkpointStatus) {
        final int checkpointFailedError = -1;
        final int checkpointSuccess = 1;
        int isSuccess = checkpointFailedError;
        final int maxRetries = 5;
        for (int retry = 0; retry < maxRetries; retry++) {
            try (TxnContext endTxn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                endTxn.putRecord(checkpointingStatusTable, tableName, checkpointStatus, null);
                if (checkpointStatus.getStatus() != CheckpointingStatus.StatusType.COMPLETED) {
                    log.error("clientCheckpointer: Marking checkpointing as failed on table {}", tableName);
                    isSuccess = checkpointFailedError; // this will stop checkpointing on first failure
                } else {
                    isSuccess = checkpointSuccess;
                }
                endTxn.delete(activeCheckpointsTable, tableName);
                endTxn.commit();
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, maxRetries)) {
                    return checkpointFailedError; // stop on non-retryable exceptions
                }
            } catch (Throwable t) {
                log.error("clientCheckpointer: encountered unexpected exception", t);
                log.error("StackTrace: {}", t.getStackTrace());
                isSuccess = checkpointFailedError;
            }
        }
        return isSuccess;
    }

    /**
     *
     * @param re - the exception this method is called on
     * @param retry - the number of times retries have been done
     * @param maxRetries - max number of times retries need to happen
     * @return - True if we should stop & return. False if we can retry!
     */
    private boolean isCriticalRuntimeException(RuntimeException re, int retry, int maxRetries) {
        log.trace("checkpointer: encountered an exception on attempt {}/{}.",
                retry, maxRetries, re);

        if (retry == maxRetries - 1) {
            log.error("checkpointer: retry exhausted.", re);
            return true;
        }

        if (re instanceof NetworkException || re.getCause() instanceof TimeoutException) {
            try {
                TimeUnit.MILLISECONDS.sleep(DistributedCompactor.CONN_RETRY_DELAY_MILLISEC);
            } catch (InterruptedException e) {
                log.error("Interrupted in network retry sleep");
                return true;
            }
        }
        if (re instanceof WrongClusterException) {
            log.error("Wrong cluster exception hit! stopping right away!");
            return true;
        }
        return false; // it is ok to retry a few times on network timeouts
    }

    public static TableName getTableName(Table<Message, Message, Message> table) {
        String fullName = table.getFullyQualifiedTableName();
        return TableName.newBuilder()
                .setNamespace(table.getNamespace())
                .setTableName(fullName.substring(fullName.indexOf("$")+1))
                .build();
    }

    /**
     * In the global checkpoint map we examine if there is a special "freeze token"
     * The sequence part of this token is overloaded with the timestamp
     * when the freeze was requested.
     * Now checkpointer being a busybody has limited patience (2 hours)
     * If the freeze request is within 2 hours it will honor it and step aside.
     * Otherwise it will angrily remove the freezeToken and continue about
     * its business.
     * @return - true if checkpointing should be skipped, false if not.
     */
    public static boolean isCheckpointFrozen(CorfuStore corfuStore, final Table<StringKey, TokenMsg, Message> chkptMap) {
        final StringKey freezeCheckpointNS = StringKey.newBuilder().setKey("freezeCheckpointNS").build();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            TokenMsg freezeToken = txn.getRecord(chkptMap, freezeCheckpointNS).getPayload();

            final long patience = 2 * 60 * 60 * 1000;
            if (freezeToken != null) {
                long now = System.currentTimeMillis();
                long frozeAt = freezeToken.getSequence();
                Date frozeAtDate = new Date(frozeAt);
                if (now - frozeAt > patience) {
                    txn.delete(chkptMap, freezeCheckpointNS);
                    log.warn("Checkpointer asked to freeze at {} but run out of patience",
                            frozeAtDate);
                } else {
                    log.warn("Checkpointer asked to freeze at {}", frozeAtDate);
                    txn.commit();
                    return true;
                }
            }
            txn.commit();
        }
        return false;
    }

    private final int loaderId = new Random().nextInt();
    private int tableNumber;
    public void justLoadData() {
        String namespace = "nsx";
        String tableName = clientName+"_BigTable"+tableNumber;
        final int numTables = 6;
        tableNumber = (tableNumber + 1)% numTables;
        int numItems =  5000;
        int batchSize = 50;
        int itemSize = 10240;
        /*** if things go wrong please uncomment this and patch...
        runtime.getTableRegistry().getRegistryTable()
               .delete(TableName.newBuilder().setTableName(tableName).setNamespace(namespace).build());
        if (batchSize == 50) {
            log.warn("Deleted table {}${}", namespace, tableName);
            return;
        }
         //*/

        try {
            TableOptions.TableOptionsBuilder optionsBuilder = TableOptions.builder();
            final Table<ExampleSchemas.ExampleTableName,
                    ExampleSchemas.ExampleTableName,
                    ExampleSchemas.ManagedMetadata> table =
                    corfuStore.openTable(
                    namespace, tableName,
                    ExampleSchemas.ExampleTableName.class,
                    ExampleSchemas.ExampleTableName.class,
                    ExampleSchemas.ManagedMetadata.class,
                    TableOptions.fromProtoSchema(ExampleSchemas.ExampleTableName.class, optionsBuilder.build())
            );

            String streamName = TableRegistry.getFullyQualifiedTableName(namespace, tableName);
            UUID streamId = CorfuRuntime.getStreamID(streamName);
            long numAddressesLoaded = runtime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS)).size();
            long tableSize = 0;
            try (TxnContext txn = corfuStore.txn(namespace)) {
                tableSize = txn.count(table);
                txn.commit();
            }
            if (numAddressesLoaded > tableSize*3) {
                log.warn("Giving up as I already loaded into table "+streamName+" addresses="+numAddressesLoaded);
                return;
            }

            /*
             * Random bytes are needed to bypass the compression.
             * If we don't use random bytes, compression will reduce the size of the payload siginficantly
             * increasing the time it takes to load data if we are trying to fill up disk.
             */
            log.warn("WARNING: Loading " + numItems + " items of " + itemSize +
                    " size in " + batchSize + " batchSized transactions into " +
                    namespace + "$" + tableName+" stream addressSpaceSize="+numAddressesLoaded);
            int itemsRemaining = numItems;
            try (TxnContext tx = corfuStore.txn(namespace)) {
                tx.clear(table);
            }
            while (itemsRemaining > 0) {
                long startTime = System.nanoTime();
                try (TxnContext tx = corfuStore.txn(namespace)) {
                    for (int j = batchSize; j > 0 && itemsRemaining > 0; j--, itemsRemaining--) {
                        byte[] array = new byte[itemSize];
                        new Random().nextBytes(array);
                        ExampleSchemas.ExampleTableName dummyVal = ExampleSchemas
                                .ExampleTableName.newBuilder().setNamespace(namespace+tableName)
                                .setTableName(new String(array, StandardCharsets.UTF_16)).build();
                        ExampleSchemas.ExampleTableName dummyKey = ExampleSchemas.ExampleTableName.newBuilder()
                                .setNamespace(Integer.toString(loaderId))
                                .setTableName(Integer.toString(j)).build();
                        tx.putRecord(table, dummyKey, dummyVal, ExampleSchemas.ManagedMetadata.getDefaultInstance());
                    }
                    CorfuStoreMetadata.Timestamp address = tx.commit();
                    long elapsedNs = System.nanoTime() - startTime;
                    log.info("loadTable: "+streamName+" Txn at address "
                            + address.getSequence() + " Items  now left " + itemsRemaining+
                            " took "+TimeUnit.NANOSECONDS.toMillis(elapsedNs)+"ms");
                    ILogData ld = runtime.getAddressSpaceView().peek(address.getSequence());
                    log.info("Item size = {}", ld.getSizeEstimate());
                }
            }
        } catch (Exception e) {
            log.error("loadTable: {} {} {} {} failed. {} stack {}", namespace, tableName, numItems, batchSize,
                    e, e.getStackTrace());
        }
    }

}

