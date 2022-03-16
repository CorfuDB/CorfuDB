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
import org.corfudb.util.serializer.*;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.*;

@Slf4j
public class DistributedCompactor {
    private final CorfuRuntime corfuRuntime;
    private final CorfuRuntime cpRuntime;
    private final String persistedCacheRoot;

    private KeyDynamicProtobufSerializer keyDynamicProtobufSerializer;
    private ISerializer protobufSerializer;
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
    public static final String NODE_TOKEN = "node-token";
    public static final String CHECKPOINT = "checkpoint";

    private final Set<String> metadataTables = new HashSet<>(
            Arrays.asList(COMPACTION_MANAGER_TABLE_NAME,
                    CHECKPOINT_STATUS_TABLE_NAME,
                    CLIENT_LIVENESS_TABLE_NAME,
                    CHECKPOINT,
                    NODE_TOKEN,
                    REGISTRY_TABLE_NAME,
                    PROTOBUF_DESCRIPTOR_TABLE_NAME));

    //TODO: have a special table list if required - like ALL_OPENED_CLUSTERING_STREAMS

    private static final StringKey COMPACTION_MANAGER_KEY = StringKey.newBuilder().setKey("CompactionManagerKey").build();
    private static final StringKey CHECKPOINT_KEY = StringKey.newBuilder().setKey("minCheckpointToken").build();

    private static final int CP_TIMEOUT = 300000;
    private static final int LIVENESS_TIMEOUT = 2000;

    private Table<StringKey, CheckpointingStatus, Message> compactionManagerTable;
    private Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable;
    private Table<UuidMsg, ClientLiveness, Message> clientLivenessTable;
    private Table<StringKey, TokenMsg, Message> checkpointTable;
    private Table<StringKey, TokenMsg, Message> nodeTokenTable;

    private final Boolean isLeader;

    private final CorfuStore corfuStore;

    public DistributedCompactor(CorfuRuntime corfuRuntime, CorfuRuntime cpRuntime, String persistedCacheRoot, Boolean isLeader) {
        this.corfuRuntime = corfuRuntime;
        this.cpRuntime = cpRuntime;
        this.persistedCacheRoot = persistedCacheRoot;
        this.isLeader = isLeader;
        this.corfuStore = new CorfuStore(corfuRuntime);

        try {
            corfuRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);
        } catch (SerializerException se) {
            // This means the protobuf serializer had not been registered yet.
            protobufSerializer = createProtobufSerializer();
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

            this.checkpointTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CHECKPOINT,
                    StringKey.class,
                    TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(TokenMsg.class));

            this.nodeTokenTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    NODE_TOKEN,
                    StringKey.class,
                    TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(TokenMsg.class));

        } catch (Exception e) {
            log.error("Caught an exception while opening Compaction management tables ", e);
        }

        scheduler.scheduleAtFixedRate(this::updateLiveness, 0, 1000, TimeUnit.MILLISECONDS);
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
                if (metadataTables.contains(table.getTableName())) {
//                    continue;
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
        keyDynamicProtobufSerializer = new KeyDynamicProtobufSerializer(cpRuntime);
        cpRuntime.getSerializers().registerSerializer(keyDynamicProtobufSerializer);

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CorfuStoreEntry compactionManagerRecord = txn.getRecord(COMPACTION_MANAGER_TABLE_NAME, COMPACTION_MANAGER_KEY);
            CheckpointingStatus managerStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();
            txn.close();
            if (managerStatus.getStatus() != StatusType.STARTED) {
                return;
            }
        }

        List<TableName> tableNames;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
             tableNames = new ArrayList<>(txn.keySet(checkpointingStatusTable)
                    .stream().collect(Collectors.toList()));
             txn.commit();
        }

        for (TableName table : tableNames) {
            CheckpointingStatus tableStatus;
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                tableStatus = (CheckpointingStatus) txn.getRecord(
                        CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();

                if (tableStatus == null || tableStatus.getStatus() != StatusType.IDLE) {
                    txn.commit();
                    continue;
                }
                //TODO: add tokens
                CheckpointingStatus startStatus = getCheckpointingStatus(StatusType.STARTED,
                        false, null, null);
                txn.putRecord(checkpointingStatusTable, table, startStatus, null);
                txn.commit();
            } catch (TransactionAbortedException e) {
                log.warn("Another compactor tried to checkpoint this table");
                continue;
            }
            count++;
            currentCPTable = table;

            CheckpointingStatus endStatus;
            try {
                Token newToken;
                if (metadataTables.contains(table.getTableName())) {
                    continue;
//                    newToken = appendCheckpoint(openTable(table, protobufSerializer, corfuRuntime), table, corfuRuntime);
                } else {
                    newToken = appendCheckpoint(openTable(table, keyDynamicProtobufSerializer, cpRuntime), table, cpRuntime);
                }
                endStatus = getCheckpointingStatus(StatusType.COMPLETED,
                        true, null,
                        TokenMsg.newBuilder().setEpoch(newToken.getEpoch()).setSequence(newToken.getSequence()).build());
            } catch (Exception e) {
                log.warn("Failed to checkpoint table: {} due to Exception: {}", table, e);
                endStatus = getCheckpointingStatus(StatusType.FAILED,
                        true, null, null);
            }

            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                txn.putRecord(checkpointingStatusTable, table, endStatus, null);
                txn.commit();
            }

            currentCPTable = null;
        }

        log.info("ClientId: {}, Checkpointed {} tables out of {}", this.clientId, count, tableNames.size());
    }

    public void finishCompactionCycle() {
        CheckpointingStatus managerStatus;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CorfuStoreEntry compactionManagerRecord = txn.getRecord(COMPACTION_MANAGER_TABLE_NAME, COMPACTION_MANAGER_KEY);
            managerStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();
            txn.commit();
        }

        if (isLeader && managerStatus.getStatus() == StatusType.STARTED) {
            List<TableName> tableNames;
            TokenMsg minToken;
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                tableNames = new ArrayList<TableName>(txn.keySet(checkpointingStatusTable)
                        .stream().collect(Collectors.toList()));
                minToken = (TokenMsg) txn.getRecord(CHECKPOINT, CHECKPOINT_KEY).getPayload();
            }
            boolean failed = false;
            TokenMsg newToken = null;
            for (TableName table : tableNames) {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                            CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                    if (tableStatus.getStatus() == StatusType.FAILED) {
                        failed = true;
                    } else if (tableStatus.getStatus() == StatusType.STARTED) {
                        long timeout = System.currentTimeMillis() + CP_TIMEOUT;
                        tableStatus = (CheckpointingStatus) txn.getRecord(
                                CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                        txn.commit();
                        while (tableStatus.getStatus() == StatusType.STARTED) {
                            if (System.currentTimeMillis() > timeout ||
                                    !validateLiveness(tableStatus.getClientId(), LIVENESS_TIMEOUT)) {
                                failed = true;
                                log.info("timeout? {} > {}", System.currentTimeMillis(), timeout);
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
                    if (tableStatus.getStatus() == StatusType.COMPLETED) {
                        newToken = (newToken == null) ? tableStatus.getEndToken() :
                                minTokenMsg(newToken, tableStatus.getEndToken());
                        log.info("Token: {}, newToken: {}", tableStatus.getEndToken(), newToken);
                    }
                }
            }
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                if (minToken == null || minToken.getEpoch() <= newToken.getEpoch() &&
                        minToken.getSequence() <= newToken.getSequence()) {
                    txn.putRecord(checkpointTable, CHECKPOINT_KEY, newToken, null);
                    log.info("mintoken: {}", minToken.toString());
                }
                txn.putRecord(compactionManagerTable, COMPACTION_MANAGER_KEY, getCheckpointingStatus(
                        failed ? StatusType.FAILED : StatusType.COMPLETED, true, null, null), null);
                txn.commit();
            }
        }
        log.info("Done with finishCompactionCycle");
    }

    private TokenMsg minTokenMsg(TokenMsg a, TokenMsg b) {
        int epochCmp = Long.compare(a.getEpoch(), b.getEpoch());
        if (epochCmp == 0) {
            return a.getSequence() <= b.getSequence() ? a : b;
        }
        return epochCmp < 0 ? a : b;
    }

    private<K, V> Token appendCheckpoint(CorfuTable<K, V> corfuTable, TableName tableName, CorfuRuntime rt) {
        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();
        mcw.addMap(corfuTable);

        long tableCkptStartTime = System.currentTimeMillis();
        log.info("Starting checkpoint namespace: {}, tableName: {}",
                tableName.getNamespace(), tableName.getTableName());

        Token trimPoint = mcw.appendCheckpoints(rt, "checkpointer");

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
            log.info("UpdatingLiveness of client: {}, current cpTable: {}", this.clientId, currentCPTable);
            if (currentCPTable != null) {
                ClientLiveness updatedClientLiveness = ClientLiveness.newBuilder()
                        .setTableName(currentCPTable)
                        .setLivenessCounter(currentClientLiveness == null ? 1 :
                                ((int) currentClientLiveness.getLivenessCounter() + 1))
                        .build();
                txn.putRecord(clientLivenessTable, this.clientId, updatedClientLiveness, null);
            }
            txn.commit();
        } catch (Exception e) {
            log.warn("Exception while updatingLiveness, {}", e.getStackTrace());
        }
    }

    private boolean validateLiveness(UuidMsg targetClient, long timeout) {
        log.info("validateLiveness");
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            log.info("validateLiveness 2");
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

    private CheckpointingStatus getCheckpointingStatus(CheckpointingStatus.StatusType statusType,
                                                       boolean endTimestamp,
                                                       @Nullable TokenMsg startToken,
                                                       @Nullable TokenMsg endToken) {
        return CheckpointingStatus.newBuilder()
                .setStatus(statusType)
                .setClientId(clientId)
                .setStartToken(startToken == null ? TokenMsg.getDefaultInstance() : startToken)
                .setEndToken(endToken==null?TokenMsg.getDefaultInstance():endToken)
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

