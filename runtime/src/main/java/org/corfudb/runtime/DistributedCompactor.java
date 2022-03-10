package org.corfudb.runtime;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuStoreMetadata.*;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.CorfuCompactorManagement.ClientLiveness;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.*;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.*;

@Slf4j
public class DistributedCompactor {
    private final CorfuRuntime corfuRuntime;
    private final String persistedCacheRoot;

    private KeyDynamicProtobufSerializer keyDynamicProtobufSerializer;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor((r) -> {
        Thread t = Executors.defaultThreadFactory().newThread(r);
        t.setName("DistributedCompactorLivenessThread");
        t.setDaemon(true);
        return t;
    });

    private final UuidMsg clientId;

    private TableName currentCPTable = null;

    private static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManager";
    private static final String CHECKPOINT_STATUS_TABLE_NAME = "CheckpointStatusTable";
    private static final String CLIENT_LIVENESS_TABLE_NAME = "ClientLivenessTable";
    private static final StringKey COMPACTION_MANAGER_KEY = StringKey.newBuilder().setKey("CompactionManagerKey").build();

    private static final int CP_TIMEOUT = 300000;
    private static final int LIVENESS_TIMEOUT = 2000;

    private Table<StringKey, CheckpointingStatus, Message> compactionManagerTable;
    private Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable;
    private Table<UuidMsg, ClientLiveness, Message> clientLivenessTable;

    private final Boolean isLeader;

    private final CorfuStore corfuStore;

    public DistributedCompactor(CorfuRuntime corfuRuntime, String persistedCacheRoot, Boolean isLeader) {
        this.corfuRuntime = corfuRuntime;
        this.persistedCacheRoot = persistedCacheRoot;
        this.isLeader = isLeader;
        this.corfuStore = new CorfuStore(corfuRuntime);

        try {
            corfuRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);
        } catch (SerializerException se) {
            // This means the protobuf serializer had not been registered yet.
            ISerializer protobufSerializer = createProtobufSerializer();
            corfuRuntime.getSerializers().registerSerializer(protobufSerializer);
        }

        clientId = UuidMsg.newBuilder()
                .setLsb(corfuRuntime.getParameters().getClientId().getLeastSignificantBits())
                .setMsb(corfuRuntime.getParameters().getClientId().getMostSignificantBits())
                .build();

        try {
            this.compactionManagerTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    COMPACTION_MANAGER_TABLE_NAME,
                    StringKey.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));

            this.checkpointingStatusTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CHECKPOINT_STATUS_TABLE_NAME,
                    TableName.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));

            this.clientLivenessTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CLIENT_LIVENESS_TABLE_NAME,
                    UuidMsg.class,
                    ClientLiveness.class,
                    null,
                    TableOptions.fromProtoSchema(ClientLiveness.class));

        } catch (Exception e) {
            log.error("Caught an exception while opening Compaction management tables ", e);
        }

        scheduler.scheduleWithFixedDelay(this::updateLiveness, 500, 1000, TimeUnit.MILLISECONDS);
    }

    public void runCompactor() {
        //TODO: add condition when the status is invalid
        // what to do when split brain happens

        init();

        try {
            if (pollForCheckpointStarted(60000)) {
                startCheckpointing();
            }
        }  catch (InterruptedException e) {
            log.error("Checkpointing hasn't started. Exiting compaction cycle. Exception: {}", e);
        }

        log.info("isLeader: {}", isLeader);
        if (isLeader) {
            finishCompactionCycle();
        }
    }

    public void init() {
        if (isLeader) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                CorfuStoreEntry compactionManagerRecord = txn.getRecord(COMPACTION_MANAGER_TABLE_NAME, COMPACTION_MANAGER_KEY);
                CheckpointingStatus currentStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();
                CheckpointingStatus newStatus = getCheckpointingStatus(StatusType.INITIALIZING,
                        false, null, null);
                txn.putRecord(compactionManagerTable, COMPACTION_MANAGER_KEY, newStatus, null);
                txn.commit();
            } catch (TransactionAbortedException transactionAbortedException) {
                log.warn("Looks like another compactor started first. Message: {}", transactionAbortedException.getCause());
                return;
            }
            startPopulating();
        }
    }

    private Boolean pollForCheckpointStarted (long timeout) throws InterruptedException{
        long timeoutUntil = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < timeoutUntil) {
            log.info("Inside pollForCPStarted");
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                CorfuStoreEntry compactionManagerRecord = txn.getRecord(COMPACTION_MANAGER_TABLE_NAME, COMPACTION_MANAGER_KEY);
                CheckpointingStatus currentStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();
                txn.commit();
                if (currentStatus.getStatus() == StatusType.STARTED) {
                    log.info("pollForCheckpointStarted: returning true");
                    return true;
                } else {
                    TimeUnit.SECONDS.sleep(timeout / 10000);
                }
            }
        }
        return false;
    }

    public void startPopulating() {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CorfuStoreEntry compactionManagerRecord = txn.getRecord(COMPACTION_MANAGER_TABLE_NAME, COMPACTION_MANAGER_KEY);
            CheckpointingStatus currentStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();
            txn.commit();
            //TODO: add more conditions
            if (!isLeader || currentStatus.getStatus() != StatusType.INITIALIZING) {
                return;
            }
        }
        List<TableName> tableNames = new ArrayList<>(corfuStore.listTables(null));
        CheckpointingStatus idleStatus = getCheckpointingStatus(StatusType.IDLE,
                false, null, null);

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            txn.clear(checkpointingStatusTable);
            for (TableName table : tableNames) {
                if (table.getTableName().equals(COMPACTION_MANAGER_TABLE_NAME) ||
                        table.getTableName().equals(CHECKPOINT_STATUS_TABLE_NAME) ||
                        table.getTableName().equals(CLIENT_LIVENESS_TABLE_NAME) ||
                        table.getTableName().equals(REGISTRY_TABLE_NAME) ||
                        table.getTableName().equals(PROTOBUF_DESCRIPTOR_TABLE_NAME)) {
                    continue;
                }
                txn.putRecord(checkpointingStatusTable, table, idleStatus, null);
            }
            txn.commit();
        }

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            //TODO: add startToken
            CheckpointingStatus managerStatus = getCheckpointingStatus(StatusType.STARTED,
                    false, null, null);
            txn.putRecord(compactionManagerTable, COMPACTION_MANAGER_KEY, managerStatus, null);
            txn.commit();
        }
    }

    public void startCheckpointing() {
        //TODO: need to cp special tables
        int count = 0;
        keyDynamicProtobufSerializer = new KeyDynamicProtobufSerializer(corfuRuntime);
        corfuRuntime.getSerializers().registerSerializer(keyDynamicProtobufSerializer);

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CorfuStoreEntry compactionManagerRecord = txn.getRecord(COMPACTION_MANAGER_TABLE_NAME, COMPACTION_MANAGER_KEY);
            CheckpointingStatus managerStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();
            txn.close();
            if (managerStatus.getStatus() != StatusType.STARTED) {
                return;
            }
        }

        List<Object> tableNames = null;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
             tableNames = new ArrayList<Object>(txn.keySet(CHECKPOINT_STATUS_TABLE_NAME)
                    .stream().collect(Collectors.toList()));
             txn.commit();
        }
        for (Object table : tableNames) {
            TableName currentTableName;
            if (table instanceof CorfuDynamicKey) {
                List<Object> valSet = new ArrayList<>(
                        ((CorfuDynamicKey) table).getKey().getAllFields().values().stream().collect(Collectors.toList()));
                currentTableName = TableName.newBuilder()
                        .setNamespace((String) valSet.get(0))
                        .setTableName((String) valSet.get(1))
                        .build();
                log.info("Start CP. table is of type CorfuDynamicKey. valSet: {}->{}, currentTableName:{}", valSet.get(0), valSet.get(1), currentTableName);
            } else {
                currentTableName = (TableName) table;
            }

            CheckpointingStatus tableStatus = null;
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                tableStatus = (CheckpointingStatus) txn.getRecord(
                        CHECKPOINT_STATUS_TABLE_NAME, currentTableName).getPayload();
                txn.commit();
            }

            if (tableStatus != null && tableStatus.getStatus() == StatusType.IDLE) {
                //TODO: add tokens
                CheckpointingStatus startStatus = getCheckpointingStatus(StatusType.STARTED,
                        false, null, null);
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    txn.putRecord(checkpointingStatusTable, currentTableName, startStatus, null);
                    txn.commit();
                }
                count++;
                currentCPTable = currentTableName;
                appendCheckpoint(openTable(currentTableName, keyDynamicProtobufSerializer), currentTableName);
                //TODO: include failure case
                CheckpointingStatus endStatus = getCheckpointingStatus(StatusType.COMPLETED,
                        true, null, null);
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    txn.putRecord(checkpointingStatusTable, currentTableName, endStatus, null);
                    txn.commit();
                }
                currentCPTable = null;
            }
        }
        log.info("ClientId: {}, Checkpointed {} tables out of {}", this.clientId, count, tableNames.size());
    }

    public void finishCompactionCycle() {
        boolean failed = false;
        CheckpointingStatus managerStatus = null;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CorfuStoreEntry compactionManagerRecord = txn.getRecord(COMPACTION_MANAGER_TABLE_NAME, COMPACTION_MANAGER_KEY);
            managerStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();
            txn.commit();
        }

        if (isLeader && managerStatus.getStatus() == StatusType.STARTED) {
            List<Object> tableNames = null;
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                tableNames = new ArrayList<Object>(txn.keySet(CHECKPOINT_STATUS_TABLE_NAME)
                        .stream().collect(Collectors.toList()));
            }
            for (Object table : tableNames) {
                if (table instanceof CorfuDynamicKey) {
                    log.info("table is of type CorfuDynamicKey. GetKey:{}", ((CorfuDynamicKey) table).getKey());
                    continue;
                }
                TableName currentTableName = (TableName) table;
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                            CHECKPOINT_STATUS_TABLE_NAME, currentTableName).getPayload();
                    if (tableStatus.getStatus() == StatusType.FAILED) {
                        failed = true;
                    } else {
                        long timeout = System.currentTimeMillis() + CP_TIMEOUT;
                        while (tableStatus.getStatus() == StatusType.STARTED) {
                            if (System.currentTimeMillis() < timeout ||
                                    !validateLiveness(tableStatus.getClientId(), LIVENESS_TIMEOUT)) {
                                failed = true;
                                break;
                            } else {
                                try {
                                    TimeUnit.MILLISECONDS.sleep(CP_TIMEOUT / 10);
                                } catch (Exception e) {
                                    log.warn("Thread interrupted: {}", e);
                                }
                            }
                        }
                    }
                    txn.putRecord(compactionManagerTable, COMPACTION_MANAGER_KEY, getCheckpointingStatus(
                            failed ? StatusType.FAILED : StatusType.COMPLETED, true, null, null), null);
                    txn.commit();
                }
            }
        }
        log.info("Done with finishCompactionCycle");
    }

    private<K, V> Token appendCheckpoint(CorfuTable<K, V> corfuTable, TableName tableName) {
        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();
        mcw.addMap(corfuTable);

        long tableCkptStartTime = System.currentTimeMillis();
        log.info("Starting checkpoint namespace: {}, tableName: {}",
                tableName.getNamespace(), tableName.getTableName());

        Token trimPoint = mcw.appendCheckpoints(corfuRuntime, "checkpointer");

        long tableCkptEndTime = System.currentTimeMillis();
        log.info("Completed checkpoint namespace: {}, tableName: {}, with {} entries in {} ms",
                tableName.getNamespace(),
                tableName.getTableName(),
                corfuTable.size(),
                (tableCkptEndTime - tableCkptStartTime));

        return trimPoint;
    }

    private void updateLiveness() {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            ClientLiveness currentClientLiveness = (ClientLiveness) txn.getRecord(CLIENT_LIVENESS_TABLE_NAME,
                    this.clientId).getPayload();
            ClientLiveness updatedClientLiveness = ClientLiveness.newBuilder()
                    .setTableName(currentCPTable)
                    .setLivenessCounter(((int) currentClientLiveness.getLivenessCounter()) + 1)
                    .build();
            txn.putRecord(clientLivenessTable, this.clientId, updatedClientLiveness, null);
            txn.commit();
        }
    }

    private boolean validateLiveness(UuidMsg targetClient, long timeout) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            ClientLiveness prevClientLiveness = (ClientLiveness) txn.getRecord(CLIENT_LIVENESS_TABLE_NAME,
                    targetClient).getPayload();
            long timeoutMillis = System.currentTimeMillis() + timeout;
            while (System.currentTimeMillis() < timeoutMillis) {
                try {
                    TimeUnit.MILLISECONDS.sleep(timeout / 10);
                } catch (Exception e) {
                    log.warn("Thread interrupted: {}", e);
                }
                ClientLiveness currentClientLiveness = (ClientLiveness) txn.getRecord(CLIENT_LIVENESS_TABLE_NAME,
                        targetClient).getPayload();
                if (currentClientLiveness.getLivenessCounter() > prevClientLiveness.getLivenessCounter()) {
                    txn.commit();
                    return true;
                }
            }
            txn.commit();
        }
        return false;
    }

    private CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord> openTable(TableName tableName,
                                                                            ISerializer serializer) {
        log.info("Opening table {} in namespace {}", tableName.getTableName(), tableName.getNamespace());
        SMRObject.Builder<CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord>> corfuTableBuilder =
                corfuRuntime.getObjectsView().build()
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
                            serializer, corfuRuntime);
            corfuTableBuilder.setArguments(mapSupplier, ICorfuVersionPolicy.MONOTONIC);
        }
        return corfuTableBuilder.open();
    }

    private CheckpointingStatus getCheckpointingStatus(CheckpointingStatus.StatusType statusType,
                                                       boolean endTimestamp,
                                                       @Nullable TokenMsg startToken,
                                                       @Nullable TokenMsg endToken) {
        return CheckpointingStatus.newBuilder()
                .setStatus(statusType)
                .setLivenessTimestamp(System.currentTimeMillis())
                .setClientId(clientId)
                .setStartToken(startToken == null ? TokenMsg.getDefaultInstance() : startToken)
                .setEndToken(endToken==null?TokenMsg.getDefaultInstance():endToken)
                .setEndTimestamp(endTimestamp?System.currentTimeMillis():0)
                .build();
    }

    private static ISerializer createProtobufSerializer() {
        ConcurrentMap<String, Class<? extends Message>> classMap = new ConcurrentHashMap<>();

        // Register the schemas of TableName, TableDescriptors, TableMetadata, ProtobufFilename/Descriptor
        // to be able to understand registry table.
        classMap.put(getTypeUrl(TableName.getDescriptor()), TableName.class);
        classMap.put(getTypeUrl(TableDescriptors.getDescriptor()),
                TableDescriptors.class);
        classMap.put(getTypeUrl(TableMetadata.getDescriptor()),
                TableMetadata.class);
        classMap.put(getTypeUrl(ProtobufFileName.getDescriptor()),
                ProtobufFileName.class);
        classMap.put(getTypeUrl(ProtobufFileDescriptor.getDescriptor()),
                ProtobufFileDescriptor.class);
        return new ProtobufSerializer(classMap);
    }
}

