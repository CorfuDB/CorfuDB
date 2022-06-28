package org.corfudb.runtime;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.KeyDynamicProtobufSerializer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

@Slf4j
public class ServerTriggeredCheckpointer extends DistributedCheckpointer {
    private final CheckpointerBuilder checkpointerBuilder;

    public ServerTriggeredCheckpointer(CheckpointerBuilder checkpointerBuilder) {
        super(checkpointerBuilder.corfuRuntime, checkpointerBuilder.clientName);
        this.checkpointerBuilder = checkpointerBuilder;
    }


    @Override
    public void checkpointTables() {
        if (!openCompactorMetadataTables(getCorfuStore())) {
            return;
        }
        //TODO: need to check compaction started?
        int countOpened = checkpointOpenedTables();

        CorfuRuntime cpRuntime = checkpointerBuilder.cpRuntime.get();
        KeyDynamicProtobufSerializer keyDynamicProtobufSerializer = new KeyDynamicProtobufSerializer(cpRuntime);
        cpRuntime.getSerializers().registerSerializer(keyDynamicProtobufSerializer);

        List<TableName> tableNames = getAllTablesToCheckpoint();
        int countUnopened = 0;
        for (TableName tableName : tableNames) {
            boolean isSuccess = tryCheckpointTable(tableName, t -> openTable(t, keyDynamicProtobufSerializer, cpRuntime));
            if (!isSuccess) {
                log.warn("Stop checkpointing after failure in {}${}", tableName.getNamespace(), tableName.getTableName());
                break;
            }
            countUnopened++;
        }
        log.info("{}: Finished checkpointing {} opened and {} unopened tables", checkpointerBuilder.clientName,
                countOpened, countUnopened);
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
        if (this.checkpointerBuilder.persistedCacheRoot.isPresent()) {
            String persistentCacheDirName = String.format("compactor_%s_%s",
                    tableName.getNamespace(), tableName.getTableName());
            Path persistedCacheLocation = Paths.get(this.checkpointerBuilder.persistedCacheRoot.get()).resolve(persistentCacheDirName);
            Supplier<StreamingMap<CorfuDynamicKey, OpaqueCorfuDynamicRecord>> mapSupplier =
                    () -> new PersistedStreamingMap<>(
                            persistedCacheLocation, PersistedStreamingMap.getPersistedStreamingMapOptions(),
                            serializer, rt);
            corfuTableBuilder.setArguments(mapSupplier, ICorfuVersionPolicy.MONOTONIC);
        }
        return corfuTableBuilder.open();
    }

    private List<TableName> getAllTablesToCheckpoint() {
        List<TableName> tablesToCheckpoint = Collections.emptyList();
        final int maxRetry = 5;
        for (int retry = 0; retry < maxRetry; retry++) {
            try (TxnContext txn = getCorfuStore().txn(CORFU_SYSTEM_NAMESPACE)) {
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
