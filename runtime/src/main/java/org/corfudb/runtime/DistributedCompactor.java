package org.corfudb.runtime;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuStoreMetadata.*;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.serializer.*;

import javax.annotation.Nullable;
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
    private final CorfuRuntime corfuRuntime;
    private final CorfuRuntime cpRuntime;
    private final String persistedCacheRoot;
    private final boolean isClient;

    private KeyDynamicProtobufSerializer keyDynamicProtobufSerializer;
    private ISerializer protobufSerializer;

    private final UuidMsg clientId;

    public static final long CONN_RETRY_DELAY_MILLISEC = 500;
    public static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManager";
    public static final String CHECKPOINT_STATUS_TABLE_NAME = "CheckpointStatusTable";
    public static final String ACTIVE_CHECKPOINTS_TABLE_NAME = "ActiveCheckpoints";
    public static final String PREVIOUS_TOKEN = "previousTokenTable";

    private static final String CLIENT_LIVENESS_TABLE_NAME = "ClientLivenessTable";
//    private static final String ACTIVE_CHECKPOINTS_TABLE_NAME = "ActiveCheckpointTable";
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

    private Table<StringKey, CheckpointingStatus, Message> compactionManagerTable;
    private Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable;
    private Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointTable;

    private final CorfuStore corfuStore;

    public DistributedCompactor(CorfuRuntime corfuRuntime, CorfuRuntime cpRuntime, String persistedCacheRoot, boolean isClient) {
        this.corfuRuntime = corfuRuntime;
        this.cpRuntime = cpRuntime;
        this.persistedCacheRoot = persistedCacheRoot;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.isClient = isClient;

        try {
            protobufSerializer = corfuRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);
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

            this.activeCheckpointTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    ACTIVE_CHECKPOINTS_TABLE_NAME,
                    TableName.class,
                    ActiveCPStreamMsg.class,
                    null,
                    TableOptions.fromProtoSchema(ActiveCPStreamMsg.class));
        } catch (Exception e) {
            log.error("Caught an exception while opening Compaction management tables ", e);
        }
    }

    public int startCheckpointing() {
        long startCp = System.currentTimeMillis();
        int count = 0;
        keyDynamicProtobufSerializer = new KeyDynamicProtobufSerializer(cpRuntime);
        cpRuntime.getSerializers().registerSerializer(keyDynamicProtobufSerializer);

        log.info("inside startCheckpointing");
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CorfuStoreEntry compactionManagerRecord = txn.getRecord(COMPACTION_MANAGER_TABLE_NAME, COMPACTION_MANAGER_KEY);
            CheckpointingStatus managerStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();
            txn.commit();
            if (managerStatus == null || managerStatus.getStatus() != StatusType.STARTED) {
                return count;
            }
        } catch (Exception e) {
            log.warn("Exception here? : ", e);
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
                //TODO: add tokens?
                CheckpointingStatus startStatus = getCheckpointingStatus(StatusType.STARTED, null, null);
                txn.putRecord(checkpointingStatusTable, table, startStatus, null);

                UUID streamId = CorfuRuntime.getCheckpointStreamIdFromName(table.getTableName());
                long streamTail = corfuRuntime.getSequencerView()
                        .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS)).getTail();
                txn.putRecord(activeCheckpointTable, table,
                        ActiveCPStreamMsg.newBuilder().setTailSequence(streamTail).setIsClientTriggered(isClient).build(),
                        null);

                txn.commit();
            } catch (TransactionAbortedException e) {
                if (e.getAbortCause() == AbortCause.CONFLICT) {
                    log.warn("Another compactor tried to checkpoint this table");
                    continue;
                } else {
                    //finishCompactionCycle will mark it as failed
                    MicroMeterUtils.measure(count, "compaction.num_tables." + clientId.toString());
                    return count;
                }
            }
            count++;
            checkpointTable(table);
        }

        long cpDuration = System.currentTimeMillis() - startCp;
        MicroMeterUtils.time(Duration.ofMillis(cpDuration), "checkpoint.total.timer",
                "clientId", clientId.toString());
        log.info("Took {} ms to checkpoint the tables by client {}", cpDuration, clientId.toString());

        MicroMeterUtils.measure(count, "compaction.num_tables." + clientId.toString());
        log.info("ClientId: {}, Checkpointed {} tables out of {}", this.clientId, count, tableNames.size());

        return count;
    }

    private void checkpointTable(TableName table) {
        CheckpointingStatus endStatus;
        try {
            Token newToken;
            if (metadataTables.contains(table.getTableName())) {
                newToken = appendCheckpoint(openTable(table, protobufSerializer, corfuRuntime), table, corfuRuntime);
            } else {
                newToken = appendCheckpoint(openTable(table, keyDynamicProtobufSerializer, cpRuntime), table, cpRuntime);
            }
            endStatus = getCheckpointingStatus(StatusType.COMPLETED, null,
                    TokenMsg.newBuilder().setEpoch(newToken.getEpoch()).setSequence(newToken.getSequence()).build());
        } catch (Exception e) {
            log.warn("Failed to checkpoint table: {} due to Exception: {}", table, e);
            endStatus = getCheckpointingStatus(StatusType.FAILED, null, null);
        }

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            txn.putRecord(checkpointingStatusTable, table, endStatus, null);
            txn.delete(ACTIVE_CHECKPOINTS_TABLE_NAME, table);
            txn.commit();
        }
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

    private CheckpointingStatus getCheckpointingStatus(CheckpointingStatus.StatusType statusType,
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

