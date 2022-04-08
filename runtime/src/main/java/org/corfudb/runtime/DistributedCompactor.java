package org.corfudb.runtime;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
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
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.*;

@Slf4j
public class DistributedCompactor {
    private final CorfuRuntime runtime;
    private final CorfuRuntime cpRuntime;
    private final String persistedCacheRoot;
    private final boolean isClient;

    private KeyDynamicProtobufSerializer keyDynamicProtobufSerializer;

    private final UuidMsg clientId;

    public static final long CONN_RETRY_DELAY_MILLISEC = 500;
    public static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManager";
    public static final String CHECKPOINT_STATUS_TABLE_NAME = "CheckpointStatusTable";
    public static final String ACTIVE_CHECKPOINTS_TABLE_NAME = "ActiveCheckpoints";
    public static final String PREVIOUS_TOKEN = "previousTokenTable";

    public static final String NODE_TOKEN = "node-token";
    public static final String CHECKPOINT = "checkpoint";

    private final Set<String> metadataTables = new HashSet<>(
            Arrays.asList(COMPACTION_MANAGER_TABLE_NAME,
                    CHECKPOINT_STATUS_TABLE_NAME,
                    ACTIVE_CHECKPOINTS_TABLE_NAME,
                    CHECKPOINT,
                    NODE_TOKEN,
                    REGISTRY_TABLE_NAME,
                    PROTOBUF_DESCRIPTOR_TABLE_NAME));

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
    private Table<StringKey, RpcCommon.TokenMsg, Message> checkpointFreezeTable = null;
    private Table<StringKey, RpcCommon.TokenMsg, Message> previousTokenTable = null;

    public DistributedCompactor(CorfuRuntime corfuRuntime, CorfuRuntime cpRuntime, String persistedCacheRoot) {
        this.runtime = corfuRuntime;
        this.cpRuntime = cpRuntime;
        this.persistedCacheRoot = persistedCacheRoot;
        this.isClient = false;

        clientId = UuidMsg.newBuilder()
                .setLsb(corfuRuntime.getParameters().getClientId().getLeastSignificantBits())
                .setMsb(corfuRuntime.getParameters().getClientId().getMostSignificantBits())
                .build();
    }

    public DistributedCompactor(CorfuRuntime corfuRuntime) {
        this.runtime = corfuRuntime;
        this.cpRuntime = null;
        this.persistedCacheRoot = null;
        this.isClient = true;

        clientId = UuidMsg.newBuilder()
                .setLsb(corfuRuntime.getParameters().getClientId().getLeastSignificantBits())
                .setMsb(corfuRuntime.getParameters().getClientId().getMostSignificantBits())
                .build();
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

            this.checkpointFreezeTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT,
                    StringKey.class,
                    RpcCommon.TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));

            this.previousTokenTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.PREVIOUS_TOKEN,
                    StringKey.class,
                    RpcCommon.TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));
        } catch (Exception e) {
            log.error("Caught an exception while opening checkpoint management tables ", e);
        }
    }

    private boolean checkGlobalCheckpointStartTrigger() {
        // This is necessary here to stop checkpointing after it has started?
        // if (isCheckpointFrozen(corfuStore, this.checkpointFreezeTable)) {return;}
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            final CorfuStoreEntry<StringKey, CheckpointingStatus, Message> compactionStatus =
                    txn.getRecord(compactionManagerTable, DistributedCompactor.COMPACTION_MANAGER_KEY);
            if (compactionStatus.getPayload() == null ||
                    compactionStatus.getPayload().getStatus() != CheckpointingStatus.StatusType.STARTED) {
                txn.commit();
                return false;
            }
            txn.commit();
        } catch (Exception e) {
            log.error("Checkpointer unable to check the main status table", e);
            return false;
        }
        return true;
    }

    public int startCheckpointing() {
        long startCp = System.currentTimeMillis();
        int count = 0;
        log.info("Starting Checkpointing in-memory tables..");
        count = checkpointOpenedTables();
        if (count <= 0) {
            log.warn("Stopping checkpointing since checkpoint of open tables has failed");
            return count;
        }

        log.info("{} in-memory tables checkpointed using protobuf serializer. Checkpointing remaining.",
                count);
        keyDynamicProtobufSerializer = new KeyDynamicProtobufSerializer(cpRuntime);
        cpRuntime.getSerializers().registerSerializer(keyDynamicProtobufSerializer);

        List<TableName> tableNames = getAllTablesToCheckpoint();
        if (tableNames == null) { // either an error or no tables to checkpoint
            return count;
        }
        for (TableName tableName : tableNames) {
            CheckpointingStatus tableStatus;
            final CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord> corfuTable =
                    openTable(tableName, keyDynamicProtobufSerializer, cpRuntime);
            final int successfulCheckpoints = tryCheckpointTable(tableName, corfuTable);
            if (successfulCheckpoints < 0) {
                // finishCompactionCycle will mark it as failed
                MicroMeterUtils.measure(count, "compaction.num_tables." + clientId.toString());
                return count;
            }
            count += successfulCheckpoints; // 0 means lock failed so don't tick up count
        }

        long cpDuration = System.currentTimeMillis() - startCp;
        MicroMeterUtils.time(Duration.ofMillis(cpDuration), "checkpoint.total.timer",
                "clientId", clientId.toString());
        log.info("Took {} ms to checkpoint the tables by client {}", cpDuration, clientId);

        MicroMeterUtils.measure(count, "compaction.num_tables." + clientId.toString());
        log.info("ClientId: {}, Checkpointed {} tables out of {}", this.clientId, count, tableNames.size());

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

    public static <K, V> Token appendCheckpoint(CorfuTable<K, V> corfuTable, TableName tableName, CorfuRuntime rt) {
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
        openCheckpointingMetadataTables();

        if (!checkGlobalCheckpointStartTrigger()) {
            return 0; // Orchestrator says checkpointing is either not needed or done.
        }

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

        Token endToken = tryAppendCheckpoint(tableName, corfuTable);

        return unlockMyCheckpointTable(tableName, endToken);
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
                if (tableToChkpt.getPayload().getStatus() == CheckpointingStatus.StatusType.IDLE) {
                    UUID streamId = CorfuRuntime.getCheckpointStreamIdFromName(
                            TableRegistry.getFullyQualifiedTableName(
                                    tableName.getNamespace(), tableName.getTableName()));
                    long streamTail = runtime.getSequencerView()
                            .getStreamAddressSpace(new StreamAddressRange(streamId,
                                    Address.MAX, Address.NON_ADDRESS)).getTail();
                    txn.putRecord(checkpointingStatusTable,
                            tableName,
                            CheckpointingStatus.newBuilder()
                                    .setStatus(CheckpointingStatus.StatusType.STARTED)
                                    .setClientId(RpcCommon.UuidMsg.newBuilder()
                                            .setLsb(runtime.getParameters().getClientId().getLeastSignificantBits())
                                            .setMsb(runtime.getParameters().getClientId().getMostSignificantBits())
                                            .build())
                                    .build(),
                            null);
                    txn.putRecord(activeCheckpointsTable, tableName,
                            ActiveCPStreamMsg.newBuilder()
                                    .setTailSequence(streamTail)
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
                log.error("clientChpt-tryLock: encountered unexpected exception", t);
                log.error("StackTrace: {}", t.getStackTrace());
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
    private Token tryAppendCheckpoint(TableName tableName, CorfuTable corfuTable) {
        final int maxRetries = 5;
        for (int retry = 0; retry < maxRetries; retry++) {
            try {
                log.info("Client checkpointing locally opened table {}${}",
                        tableName.getNamespace(), tableName.getTableName());
                return DistributedCompactor.appendCheckpoint(corfuTable, tableName, runtime);
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, maxRetries)) {
                    return null; // stop on non-retryable exceptions
                }
            } catch (Throwable t) {
                log.error("clientCheckpointer: encountered unexpected exception", t);
                log.error("StackTrace: {}", t.getStackTrace());
            }
        }
        return null;
    }

    /**
     * Mark the checkpointed table as either done or failed based on endToken
     * @param tableName - protobuf name of the table just checkpointed
     * @param endToken - final token at which the checkpoint snapshot was written
     * @return - 1 on success, 0 or negative value on failure
     */
    private int unlockMyCheckpointTable(TableName tableName, Token endToken) {
        final int checkpointFailedError = -1;
        final int checkpointSuccess = 1;
        int isSuccess = checkpointFailedError;
        final int maxRetries = 5;
        for (int retry = 0; retry < maxRetries; retry++) {
            try (TxnContext endTxn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                if (endToken == null) {
                    log.error("clientCheckpointer: Marking checkpointing as failed on table {}", tableName);
                    endTxn.putRecord(checkpointingStatusTable,
                            tableName,
                            CheckpointingStatus.newBuilder()
                                    .setStatus(CheckpointingStatus.StatusType.FAILED)
                                    .setClientId(RpcCommon.UuidMsg.newBuilder()
                                            .setLsb(runtime.getParameters().getClientId().getLeastSignificantBits())
                                            .setMsb(runtime.getParameters().getClientId().getMostSignificantBits())
                                            .build())
                                    .build(),
                            null);
                    isSuccess = checkpointFailedError; // this will stop checkpointing on first failure
                } else {
                    endTxn.putRecord(checkpointingStatusTable,
                            tableName,
                            CheckpointingStatus.newBuilder()
                                    .setStatus(CheckpointingStatus.StatusType.COMPLETED)
                                    .setClientId(RpcCommon.UuidMsg.newBuilder()
                                            .setLsb(runtime.getParameters().getClientId().getLeastSignificantBits())
                                            .setMsb(runtime.getParameters().getClientId().getMostSignificantBits())
                                            .build())
                                    .setEndToken(RpcCommon.TokenMsg.newBuilder()
                                            .setEpoch(endToken.getEpoch())
                                            .setSequence(endToken.getSequence())
                                            .build())
                                    .build(),
                            null);
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

}

