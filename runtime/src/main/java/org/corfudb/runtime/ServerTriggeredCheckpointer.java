package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.OpaqueCorfuDynamicRecord;
import org.corfudb.runtime.collections.PersistedCorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.KeyDynamicProtobufSerializer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.corfudb.runtime.CorfuOptions.ConsistencyModel.READ_COMMITTED;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class ServerTriggeredCheckpointer extends DistributedCheckpointer {
    private final CheckpointerBuilder checkpointerBuilder;

    public ServerTriggeredCheckpointer(CheckpointerBuilder checkpointerBuilder,
                                       CorfuStore corfuStore, CompactorMetadataTables compactorMetadataTables) {
        super(checkpointerBuilder.corfuRuntime, checkpointerBuilder.clientName, corfuStore, compactorMetadataTables);
        this.checkpointerBuilder = checkpointerBuilder;
    }

    @Override
    public void checkpointTables() {
        checkpointOpenedTables();

        CorfuRuntime cpRuntime = checkpointerBuilder.cpRuntime.get();
        KeyDynamicProtobufSerializer keyDynamicProtobufSerializer = new KeyDynamicProtobufSerializer(cpRuntime);
        cpRuntime.getSerializers().registerSerializer(keyDynamicProtobufSerializer);

        List<TableName> tableNames = getAllTablesToCheckpoint();
        for (TableName tableName : tableNames) {
            boolean isSuccess = tryCheckpointTable(tableName, t -> getCheckpointWriter(t, keyDynamicProtobufSerializer));
            if (!isSuccess) {
                log.warn("Stop checkpointing after failure");
                break;
            }
        }
        log.info("{}: Finished checkpointing tables", checkpointerBuilder.clientName);
    }

    private CheckpointWriter<ICorfuTable<?,?>> getCheckpointWriter(
            TableName tableName,
            KeyDynamicProtobufSerializer keyDynamicProtobufSerializer) {

        UUID streamId = FullyQualifiedTableName.streamId(tableName).getId();
        ICorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord> corfuTable = openTable(
                tableName, keyDynamicProtobufSerializer, checkpointerBuilder.cpRuntime.get());
        CheckpointWriter<ICorfuTable<?,?>> cpw =
                new CheckpointWriter(checkpointerBuilder.cpRuntime.get(), streamId, "ServerCheckpointer", corfuTable);
        cpw.setSerializer(keyDynamicProtobufSerializer);
        return cpw;
    }

    private ICorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord> openTable(TableName tableName,
                                                                            ISerializer serializer,
                                                                            CorfuRuntime rt) {
        log.info("Opening table {} in namespace {}", tableName.getTableName(), tableName.getNamespace());

        if (this.checkpointerBuilder.persistedCacheRoot.isPresent()) {
            log.info("Opening table in disk mode");
            final String persistentCacheDirName = String.format("compactor_%s_%s",
                    tableName.getNamespace(), tableName.getTableName());
            final Path persistedCacheLocation = Paths.get(this.checkpointerBuilder.persistedCacheRoot
                    .get()).resolve(persistentCacheDirName);
            final PersistenceOptions persistenceOptions = PersistenceOptions.builder()
                    .consistencyModel(READ_COMMITTED)
                    .dataPath(persistedCacheLocation)
                    .build();

            return rt.getObjectsView().<PersistedCorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord>>build()
                    .setStreamName(FullyQualifiedTableName.build(tableName).toFqdn())
                    .setSerializer(serializer)
                    .addOpenOption(ObjectOpenOption.NO_CACHE)
                    .setTypeToken(PersistedCorfuTable.<CorfuDynamicKey, OpaqueCorfuDynamicRecord>getTypeToken())
                    .setArguments(persistenceOptions, serializer)
                    .open();
        } else {
            return rt.getObjectsView()
                            .<PersistentCorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord>>build()
                            .setStreamName(FullyQualifiedTableName.build(tableName).toFqdn())
                            .setSerializer(serializer)
                            .addOpenOption(ObjectOpenOption.NO_CACHE)
                            .setTypeToken(PersistentCorfuTable.<CorfuDynamicKey, OpaqueCorfuDynamicRecord>getTypeToken())
                            .open();
        }
    }

    private List<TableName> getAllTablesToCheckpoint() {
        List<TableName> tablesToCheckpoint = Collections.emptyList();
        final int maxRetry = 5;
        for (int retry = 0; retry < maxRetry; retry++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                tablesToCheckpoint = new ArrayList<>(txn.keySet(CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME));
                txn.commit();
                break;
            } catch (RuntimeException re) {
                if (!isCriticalRuntimeException(re, retry, maxRetry)) {
                    return tablesToCheckpoint;
                }
            } catch (Exception e) {
                log.error("getAllTablesToCheckpoint: encountered unexpected exception", e);
            }
        }
        return tablesToCheckpoint;
    }
}
