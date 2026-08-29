package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class MockLivenessUpdater implements LivenessUpdater {

    private ScheduledExecutorService executorService;
    private static final Duration UPDATE_INTERVAL = Duration.ofMillis(250);

    private Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointsTable = null;
    private Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable = null;

    private final CorfuStore corfuStore;

    private TableName tableName = null;

    public MockLivenessUpdater(CorfuStore corfuStore) {
        this.corfuStore = corfuStore;
        try {
            this.activeCheckpointsTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME,
                    TableName.class,
                    ActiveCPStreamMsg.class,
                    null,
                    TableOptions.fromProtoSchema(ActiveCPStreamMsg.class));

            this.checkpointingStatusTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME,
                    TableName.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));
        } catch (Exception e) {
            log.error("Caught an exception while opening metadata tables ", e);
        }
    }

    @Override
    public void start() {
        //No action done
    }

    @Override
    public void setCurrentTable(TableName tableName) {
        this.tableName = tableName;
        // update validity counter every 250ms
        executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay(() -> {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                ActiveCPStreamMsg currentStatus =
                        txn.getRecord(activeCheckpointsTable, tableName).getPayload();
                ActiveCPStreamMsg newStatus = ActiveCPStreamMsg.newBuilder()
                        .setSyncHeartbeat(currentStatus.getSyncHeartbeat() + 1)
                        .build();
                txn.putRecord(activeCheckpointsTable, tableName, newStatus, null);
                txn.commit();
                log.info("Updated liveness for table {} to {}", tableName, currentStatus.getSyncHeartbeat() + 1);
            } catch (Exception e) {
                log.error("Unable to update liveness for table: {}, e ", tableName, e);
            }
        }, 0, UPDATE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void changeStatus() {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus tableStatus =
                    txn.getRecord(checkpointingStatusTable, tableName).getPayload();
            if (tableStatus == null || tableStatus.getStatus() != CheckpointingStatus.StatusType.STARTED) {
                txn.commit();
                return;
            }
            CheckpointingStatus newStatus = CheckpointingStatus.newBuilder()
                    .setStatus(CheckpointingStatus.StatusType.COMPLETED)
                    .setClientName(tableStatus.getClientName())
                    .setTimeTaken(tableStatus.getTimeTaken())
                    .build();
            txn.putRecord(checkpointingStatusTable, tableName, newStatus, null);
            txn.delete(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, tableName);
            txn.commit();
        } catch (Exception e) {
            log.error("Unable to mark status as COMPLETED for table: {}, {} StackTrace: {}",
                    tableName, e, e.getStackTrace());
        }
    }

    @Override
    public void unsetCurrentTable() {
        executorService.shutdownNow();
        changeStatus();
    }

    @Override
    public void shutdown() {
        //No action done
    }
}
