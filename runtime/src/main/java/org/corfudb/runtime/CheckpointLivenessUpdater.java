package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class CheckpointLivenessUpdater implements LivenessUpdater {
    private final CorfuStore corfuStore;
    private final ScheduledExecutorService executorService;

    private Optional<TableName> currentTable = Optional.empty();

    private static final Duration UPDATE_INTERVAL = Duration.ofSeconds(15);

    public CheckpointLivenessUpdater(CorfuStore corfuStore) {
        this.corfuStore = corfuStore;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay(this::addTask, UPDATE_INTERVAL.toMillis() / 2,
                UPDATE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    private Table<TableName, ActiveCPStreamMsg, Message> getActiveCheckpointsTable()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        return this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME,
                TableName.class,
                ActiveCPStreamMsg.class,
                null,
                TableOptions.fromProtoSchema(ActiveCPStreamMsg.class));
    }

    private void addTask() {
        if (!currentTable.isPresent()) {
            return;
        }
        executorService.execute(() -> {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                ActiveCPStreamMsg currentStatus = (ActiveCPStreamMsg)
                        txn.getRecord(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, currentTable.get()).getPayload();
                ActiveCPStreamMsg newStatus = ActiveCPStreamMsg.newBuilder()
                        .setSyncHeartbeat(currentStatus.getSyncHeartbeat() + 1)
                        .build();
                Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointsTable = getActiveCheckpointsTable();
                // update validity counter for the current table
                txn.putRecord(activeCheckpointsTable, currentTable.get(), newStatus, null);
                txn.commit();
            } catch (Exception e) {
                log.error("Unable to update liveness for table: {} due to exception: {}", currentTable, e.getStackTrace());
            }
        });
    }

    @Override
    public void updateLiveness(TableName tableName) {
        this.currentTable = Optional.of(tableName);
    }

    @Override
    public void notifyOnSyncComplete() {
        currentTable = Optional.empty();
    }

    public void shutdown() {
        executorService.shutdownNow();
    }
}
