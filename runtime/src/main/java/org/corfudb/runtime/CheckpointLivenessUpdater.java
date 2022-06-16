package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class CheckpointLivenessUpdater implements LivenessUpdater {
    private ScheduledExecutorService executorService;
    private static final Duration UPDATE_INTERVAL = Duration.ofSeconds(15);

    private final CorfuStore corfuStore;
    private Table<TableName, CorfuCompactorManagement.ActiveCPStreamMsg, Message> activeCheckpointsTable = null;

    public CheckpointLivenessUpdater(CorfuStore corfuStore) {

        this.corfuStore = corfuStore;
        try {
            this.activeCheckpointsTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME,
                    TableName.class,
                    CorfuCompactorManagement.ActiveCPStreamMsg.class,
                    null,
                    TableOptions.fromProtoSchema(CorfuCompactorManagement.ActiveCPStreamMsg.class));
        } catch (Exception e) {
            log.error("Caught an exception while opening checkpoint management tables ", e);
        }
    }

    @Override
    public void updateLiveness(TableName tableName) {
        // update validity counter every 15s
        executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay(() -> {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                CorfuCompactorManagement.ActiveCPStreamMsg currentStatus =
                        txn.getRecord(activeCheckpointsTable, tableName).getPayload();
                CorfuCompactorManagement.ActiveCPStreamMsg newStatus = CorfuCompactorManagement.ActiveCPStreamMsg.newBuilder()
                        .setSyncHeartbeat(currentStatus.getSyncHeartbeat() + 1)
                        .setIsClientTriggered(currentStatus.getIsClientTriggered())
                        .build();
                txn.putRecord(activeCheckpointsTable, tableName, newStatus, null);
                txn.commit();
            } catch (Exception e) {
                log.error("Unable to update liveness for table: {}", tableName);
            }
        }, UPDATE_INTERVAL.toMillis() / 2, UPDATE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void notifyOnSyncComplete() {
        executorService.shutdownNow();
    }
}
