package org.corfudb.compactor;


import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.Tuple;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.proto.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalRetry;
import org.corfudb.util.retry.RetryNeededException;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.KeyDynamicProtobufSerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.util.serializer.Serializers;
import org.corfudb.utils.lock.*;
import org.corfudb.utils.lock.states.*;
import org.rocksdb.Checkpoint;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;
import static org.corfudb.runtime.view.TableRegistry.getTypeUrl;

@Slf4j
public class DistributedCompactorWithLock {

    private final CorfuRuntime corfuRuntime;
    private final String persistedCacheRoot;
    private final RpcCommon.UuidMsg clientId;
    private TableName currentCPTable = null;

    private static final LinkedBlockingQueue<Tuple<EventStatus, Tuple<Token, TableName>>> eventQueue = new LinkedBlockingQueue<>();

    private KeyDynamicProtobufSerializer keyDynamicProtobufSerializer;

    private static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManager";
    private static final String CHECKPOINT_STATUS_TABLE_NAME = "CheckpointStatusTable";
    private static final String COMPACTION_MANAGER_KEY = "CompactionManagerKey";

    private boolean isLeader;
    private final static int LOCK_LEASE_DURATION = 5;
    /**
     * Fraction of Lease Duration for Lease Renewal
     */
    private static final int RENEWAL_LEASE_FRACTION = 4;

    /**
     * Fraction of Lease Duration for Lease Monitoring
     */
    private static final int MONITOR_LEASE_FRACTION = 10;

    /**
     * Lock-related configuration parameters
     */
    private static final String LOCK_GROUP = "Distributed_Compactor_Group";
    private static final String LOCK_NAME = "Distributed_Compactor_Lock";
    private static LockClient lockClient;

    private Token previousToken;
    private CorfuTable<TableName, CheckpointingStatus> checkpointStatusTable;
    private CorfuTable<String, CheckpointingStatus> compactionManagerTable;

    public enum EventStatus {
        LOCK_ACQUIRED,
        LOCK_REVOKED,
        START_CHECKPOINTING,
        UPDATE_TOKEN
    }

    DistributedCompactorWithLock(CorfuRuntime corfuRuntime, String persistedCacheRoot) {
        this.corfuRuntime = corfuRuntime;
        this.persistedCacheRoot = persistedCacheRoot;

        try {
            corfuRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);
        } catch (SerializerException se) {
            // This means the protobuf serializer had not been registered yet.
            ISerializer protobufSerializer = createProtobufSerializer();
            corfuRuntime.getSerializers().registerSerializer(protobufSerializer);
        }

        clientId = RpcCommon.UuidMsg.newBuilder()
                .setLsb(corfuRuntime.getParameters().getClientId().getLeastSignificantBits())
                .setMsb(corfuRuntime.getParameters().getClientId().getMostSignificantBits())
                .build();

        checkpointStatusTable = getCheckpointStatusTable();
        compactionManagerTable = getCompactionManagerTable();

        subscribeListener();
    }

    public void init() throws InterruptedException {
        //Compute min token based on trim
        registerForLock();
        processEventQueue();
    }

    private void processEventQueue() {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.execute(() -> {
            while (true) {
                Tuple<EventStatus, Tuple<Token, TableName>> event = null;
                try {
                    event = eventQueue.poll(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    log.warn("processEventQueue Interrupted: {}", e.getStackTrace());
                }
                if (event.first.equals(EventStatus.LOCK_ACQUIRED)) {
                    processLockAcquire();
                } else if (event.first.equals(EventStatus.START_CHECKPOINTING)) {
                    startCheckpointing();
                    if (isLeader) {
                        finishCompactionCycle();
                    }
                } else if (event.first.equals(EventStatus.UPDATE_TOKEN)) {
                    updateLiveness(event.second.first, event.second.second);
                }
            }
        });
    }

    public void onLeadershipAcquire() {
        isLeader = true;
        CheckpointingStatus statusRecord = compactionManagerTable.get(COMPACTION_MANAGER_KEY);

//        If CompactionManagerKey is STARTED/INITIALIZED and node id is the current node,
//        that means the current node crashed somehow - So perform starting phase
//        If CompactionManagerKey is STARTED/INITIALIZED and node id is not current node,
//        but distributed lock is not acquired by anyone, then the coordinator seems to be
        //TODO: add current token for start token
        CheckpointingStatus newStatus = getCheckpointingStatus(
                CheckpointingStatus.StatusType.INITIALIZING,
                false, null, null);
        try {
            corfuRuntime.getObjectsView().TXBegin();
            compactionManagerTable.put(COMPACTION_MANAGER_KEY, newStatus);
            corfuRuntime.getObjectsView().TXEnd();
        } catch (TransactionAbortedException transactionAbortedException) {
            log.warn("Looks like another compactor started first. Message: {}", transactionAbortedException.getCause());
            return;
        }
        startPopulating();
    }

    private void startPopulating() {

        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        List<TableName> tableNames = new ArrayList<>(corfuStore.listTables(null));

        CheckpointingStatus cpStatus = getCheckpointingStatus(CheckpointingStatus.StatusType.IDLE,
                false, null, null);

        //TODO: add with special tables
        corfuRuntime.getObjectsView().TXBegin();
        checkpointStatusTable.clear();
        for (TableName table : tableNames) {
            checkpointStatusTable.put(table, cpStatus);
        }
        //TODO: add startToken
        CheckpointingStatus managerStatus = getCheckpointingStatus(CheckpointingStatus.StatusType.STARTED,
                false, null, null);
        compactionManagerTable.put(COMPACTION_MANAGER_KEY, managerStatus);
        corfuRuntime.getObjectsView().TXEnd();
    }

    public void startCheckpointing() {

        keyDynamicProtobufSerializer = new KeyDynamicProtobufSerializer(corfuRuntime);
        corfuRuntime.getSerializers().registerSerializer(keyDynamicProtobufSerializer);

        // TODO
        log.info("StartCheckpointing: compactionManagerTable : {}", compactionManagerTable.get(COMPACTION_MANAGER_KEY).toString());
        if (compactionManagerTable.get(COMPACTION_MANAGER_KEY).getStatus() != CheckpointingStatus.StatusType.STARTED) {
            return;
        }

        List<TableName> tableNames = new ArrayList<>(checkpointStatusTable.keySet().stream().collect(Collectors.toList()));
            for(
        TableName table :tableNames)

        {
            if (checkpointStatusTable.get(table).getStatus() == CheckpointingStatus.StatusType.IDLE) {
                //TODO: add tokens
                CheckpointingStatus startStatus = getCheckpointingStatus(CheckpointingStatus.StatusType.STARTED,
                        false, null, null);
                corfuRuntime.getObjectsView().TXBegin();
                checkpointStatusTable.put(table, startStatus);
                corfuRuntime.getObjectsView().TXEnd();

                currentCPTable = table;
                appendCheckpoint(openTable(table, keyDynamicProtobufSerializer), table);

                CheckpointingStatus endStatus = getCheckpointingStatus(CheckpointingStatus.StatusType.COMPLETED,
                        true, null, null);
                corfuRuntime.getObjectsView().TXBegin();
                checkpointStatusTable.put(table, endStatus);
                corfuRuntime.getObjectsView().TXEnd();
                currentCPTable = null;
            }
        }
    }

    public void finishCompactionCycle() {
        boolean failed = false;
        if (compactionManagerTable.get(COMPACTION_MANAGER_KEY).getStatus() == CheckpointingStatus.StatusType.STARTED) {
            List<TableName> tableNames = new ArrayList<>(checkpointStatusTable.keySet().stream().collect(Collectors.toList()));
            for (TableName table : tableNames) {
                if (checkpointStatusTable.get(table).getStatus() == CheckpointingStatus.StatusType.FAILED) {
                    failed = true;
                } else if (checkpointStatusTable.get(table).getStatus() == CheckpointingStatus.StatusType.STARTED) {
                    //TODO: redo
                    long timeout = 30000;
                    long endTime = System.currentTimeMillis() + timeout;
                    while (System.currentTimeMillis() < endTime) {
                        if (!validateLiveness(table, timeout/10)) {
                            failed = true;
                            break;
                        }
                        try {
                            TimeUnit.MILLISECONDS.sleep(timeout / 10);
                        } catch (InterruptedException e) {
                            log.warn("Thread interrupted in finishCompactionCycle: {}", e.getCause());
                        }
                    }
                }
            }
            compactionManagerTable.put(COMPACTION_MANAGER_KEY, getCheckpointingStatus(
                    failed? CheckpointingStatus.StatusType.COMPLETED: CheckpointingStatus.StatusType.FAILED, true, null, null));
        }
        log.info("Done with finishCompactionCycle");
    }

    private<K, V> Token appendCheckpoint(CorfuTable<K, V> corfuTable, TableName tableName) {
        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();
        mcw.addMap(corfuTable);

        long tableCkptStartTime = System.currentTimeMillis();
        log.info("Starting checkpoint namespace: {}, tableName: {}",
                tableName.getNamespace(), tableName.getTableName());

        Token trimPoint = mcw.appendCheckpoints1(corfuRuntime, "checkpointer");

        long tableCkptEndTime = System.currentTimeMillis();
        log.info("Completed checkpoint namespace: {}, tableName: {}, with {} entries in {} ms",
                tableName.getNamespace(),
                tableName.getTableName(),
                corfuTable.size(),
                (tableCkptEndTime - tableCkptStartTime));

        return trimPoint;
    }

    private void updateLiveness(Token token, TableName tableName) {
        CheckpointingStatus currentStatus = compactionManagerTable.get(COMPACTION_MANAGER_KEY);
        //Find a way to add current token
        RpcCommon.TokenMsg currentToken = RpcCommon.TokenMsg.newBuilder()
                .setEpoch(token.getEpoch())
                .setSequence(token.getSequence())
                .build();

        if (currentCPTable != null && checkpointStatusTable.get(currentCPTable).getStatus() == CheckpointingStatus.StatusType.STARTED) {
            if (currentCPTable.equals(tableName)) {
                checkpointStatusTable.put(currentCPTable, getCheckpointingStatus(currentStatus.getStatus(),
                        false, currentStatus.getStartToken(), currentToken));
            }
        }
    }

    // TODO: check if token is > previous min token of the cycle
    // timeout is in ms
    private boolean validateLiveness(TableName table, long timeout){
        long timeoutTime = System.currentTimeMillis() + timeout;
        try {
            while (System.currentTimeMillis() < timeoutTime) {
                CheckpointingStatus checkpointingStatus = checkpointStatusTable.get(table);
                if (checkpointingStatus.getEndToken().getEpoch() >= previousToken.getEpoch() &&
                        checkpointingStatus.getEndToken().getSequence() > previousToken.getSequence()) {
                    return true;
                }
                TimeUnit.MILLISECONDS.sleep(timeout / 10);
            }
        } catch (Exception e) {
            log.warn("Exception in validateLiveness: {}", e.getCause());
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

    private CorfuTable<String, Token> getCheckpointMap() {
        return org.corfudb.runtime.CorfuRuntimeHelper.getCheckpointMap(corfuRuntime);
    }

    private CorfuTable<String, Token> getNodeTrimTokenMap() {
        return CorfuRuntimeHelper.getNodeTrimTokenMap(corfuRuntime);
    }

    private CorfuTable<TableName, CheckpointingStatus> getCheckpointStatusTable() {
        return corfuRuntime.getObjectsView()
                .build()
                .setStreamName(CHECKPOINT_STATUS_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<TableName, CheckpointingStatus>>() {})
                .setSerializer(Serializers.JSON)
                .open();
    }

    private CorfuTable<String, CheckpointingStatus> getCompactionManagerTable() {
        return corfuRuntime.getObjectsView()
                .build()
                .setStreamName(COMPACTION_MANAGER_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, CheckpointingStatus>>() {})
                .setSerializer(Serializers.JSON)
                .open();
    }

    private CheckpointingStatus getCheckpointingStatus(CheckpointingStatus.StatusType statusType,
                                                                                boolean endTimestamp,
                                                                                @Nullable RpcCommon.TokenMsg startToken,
                                                                                @Nullable RpcCommon.TokenMsg endToken) {
        return CheckpointingStatus.newBuilder()
                .setStatus(statusType)
                .setLivenessTimestamp(System.currentTimeMillis())
                .setClientId(clientId)
                .setStartToken(startToken == null ? RpcCommon.TokenMsg.getDefaultInstance() : startToken)
                .setEndToken(endToken==null? RpcCommon.TokenMsg.getDefaultInstance():endToken)
                .setEndTimestamp(endTimestamp?System.currentTimeMillis():0)
                .build();
    }

    private static ISerializer createProtobufSerializer() {
        ConcurrentMap<String, Class<? extends Message>> classMap = new ConcurrentHashMap<>();

        // Register the schemas of TableName, TableDescriptors, TableMetadata, ProtobufFilename/Descriptor
        // to be able to understand registry table.
        classMap.put(getTypeUrl(TableName.getDescriptor()), TableName.class);
        classMap.put(getTypeUrl(CorfuStoreMetadata.TableDescriptors.getDescriptor()),
                CorfuStoreMetadata.TableDescriptors.class);
        classMap.put(getTypeUrl(CorfuStoreMetadata.TableMetadata.getDescriptor()),
                CorfuStoreMetadata.TableMetadata.class);
        classMap.put(getTypeUrl(CorfuStoreMetadata.ProtobufFileName.getDescriptor()),
                CorfuStoreMetadata.ProtobufFileName.class);
        classMap.put(getTypeUrl(CorfuStoreMetadata.ProtobufFileDescriptor.getDescriptor()),
                CorfuStoreMetadata.ProtobufFileDescriptor.class);
        return new ProtobufSerializer(classMap);
    }

    /**
     * Process lock acquisition event
     */
    private void processLockAcquire() {
        log.debug("Lock acquired");
        onLeadershipAcquire();
    }

    public static void input(EventStatus eventStatus) throws InterruptedException {
        eventQueue.put(new Tuple<>(eventStatus, null));
    }
    public static void input(EventStatus eventStatus, Token token, TableName tableName) throws InterruptedException {
        eventQueue.put(new Tuple<>(eventStatus, new Tuple<>(token, tableName)));
    }

    private void registerForLock() {
        try {
            Lock.setLeaseDuration(LOCK_LEASE_DURATION);
            LockClient.setDurationBetweenLockMonitorRuns(LOCK_LEASE_DURATION / MONITOR_LEASE_FRACTION);
            LockState.setDurationBetweenLeaseRenewals(LOCK_LEASE_DURATION / RENEWAL_LEASE_FRACTION);
            HasLeaseState.setDurationBetweenLeaseChecks(LOCK_LEASE_DURATION / MONITOR_LEASE_FRACTION);

            UUID lockId = new UUID(clientId.getMsb(), clientId.getLsb());
            IRetry.build(IntervalRetry.class, () -> {
                try {
                    lockClient = new LockClient(lockId, corfuRuntime);
                    // Callback on lock acquisition or revoke
                    LockListener distributedCompactorLockListener = new DistributedCompactorLockListener();
                    // Register Interest on the shared Log Replication Lock
                    lockClient.registerInterest(LOCK_GROUP, LOCK_NAME, distributedCompactorLockListener);
                } catch (Exception e) {
                    log.error("Error while attempting to register interest on log replication lock {}:{}",
                            LOCK_GROUP, LOCK_NAME, e);
                    throw new RetryNeededException();
                }

                log.debug("Registered to lock, client msb={}, lsb={}", lockId.getMostSignificantBits(),
                        lockId.getLeastSignificantBits());
                return null;
            }).run();
        } catch (InterruptedException e) {
            log.error("Unrecoverable exception when attempting to register interest on log replication lock.", e);
            throw new UnrecoverableCorfuInterruptedError(e);
        }
    }

    private void subscribeListener() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        corfuStore.subscribeListener(new DistributedCompactorEventListener(), null, COMPACTION_MANAGER_TABLE_NAME);
    }
}
