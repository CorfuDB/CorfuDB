package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class DistributedCheckpointerHelper {

    private final CorfuStore corfuStore;

    @Getter
    private final CompactorMetadataTables compactorMetadataTables;

    public DistributedCheckpointerHelper(CorfuStore corfuStore) throws Exception {
        this.corfuStore = corfuStore;

        // Open all compactor related tables
        this.compactorMetadataTables = new CompactorMetadataTables(corfuStore);
    }

    public static enum UpdateAction {
        PUT,
        DELETE
    }

    public void disableCompaction() {
        updateCompactionControlsTable(
                CompactorMetadataTables.DISABLE_COMPACTION, UpdateAction.PUT);
        log.info("Disabled compaction");
    }

    public void enableCompaction() {
        updateCompactionControlsTable(CompactorMetadataTables.DISABLE_COMPACTION, UpdateAction.DELETE);
        log.info("Disabled compaction");
    }

    public void freezeCompaction() {
        updateCompactionControlsTable(CompactorMetadataTables.FREEZE_TOKEN, UpdateAction.PUT);
    }

    public void unfreezeCompaction() {
        updateCompactionControlsTable(CompactorMetadataTables.FREEZE_TOKEN, UpdateAction.DELETE);
    }

    public void instantTrigger(boolean trim) {
        if (trim) {
            updateCompactionControlsTable(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM, UpdateAction.PUT);
        } else {
            updateCompactionControlsTable(CompactorMetadataTables.INSTANT_TIGGER, UpdateAction.PUT);
        }
    }

    private void updateCompactionControlsTable(StringKey stringKey, UpdateAction action) {

        Table<StringKey, RpcCommon.TokenMsg, Message> compactionControlsTable =
                compactorMetadataTables.getCompactionControlsTable();
        for (int retry = 1; retry <= CompactorMetadataTables.MAX_RETRIES; retry++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                updateCompactionControlsTable(txn, compactionControlsTable, stringKey, action);
                txn.commit();
            } catch (Exception e) {
                if (retry == CompactorMetadataTables.MAX_RETRIES) {
                    log.error("Failed to update CompactionControlsTable with Key:{}, Action:{} after retry.",
                            stringKey, action, e);
                    throw e;
                }
                log.warn("Unable to write Key:{}, Action:{} to CompactionControlsTable. Retrying for {} / {}.",
                        stringKey, action, retry, CompactorMetadataTables.MAX_RETRIES);
                try {
                    TimeUnit.SECONDS.sleep(CompactorMetadataTables.TABLE_UPDATE_RETRY_SLEEP_SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ie);
                }
            }
        }
        log.info("Updated ");
    }
    private void updateCompactionControlsTable(TxnContext txn,
                                              Table<StringKey, RpcCommon.TokenMsg, Message> checkpointTable,
                                              StringKey stringKey,
                                              UpdateAction action) {
        if (action == UpdateAction.PUT) {
            txn.putRecord(checkpointTable, stringKey,
                    RpcCommon.TokenMsg.newBuilder().setSequence(System.currentTimeMillis()).build(), null);
        } else if (action == UpdateAction.DELETE) {
            txn.delete(checkpointTable, stringKey);
        }
    }

    public boolean isCheckpointFrozen() {
        boolean isFrozen;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            isFrozen = isCheckpointFrozen(txn);
            txn.commit();
        }
        return isFrozen;
    }

    public boolean isCheckpointFrozen(TxnContext txn) {
        RpcCommon.TokenMsg freezeToken = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                CompactorMetadataTables.FREEZE_TOKEN).getPayload();
        final long patience = 2 * 60 * 60 * 1000;
        if (freezeToken != null) {
            long now = System.currentTimeMillis();
            long frozeAt = freezeToken.getSequence();
            Date frozeAtDate = new Date(frozeAt);
            if (now - frozeAt > patience) {
                txn.delete(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN);
                log.warn("Checkpointer asked to freeze at {} but run out of patience",
                        frozeAtDate);
            } else {
                log.warn("Checkpointer asked to freeze at {}", frozeAtDate);
                return true;
            }
        }
        return false;
    }

    public boolean isCompactionDisabled() {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            RpcCommon.TokenMsg disableCompaction = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                    CompactorMetadataTables.DISABLE_COMPACTION).getPayload();
            txn.commit();
            if (disableCompaction != null) {
                return true;
            }
        }
        return false;
    }

    public boolean hasCompactionStarted() {
        //This is necessary here to stop checkpointing after it has started
        if (isCompactionDisabled() || isCheckpointFrozen()) {
            return false;
        }
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            final CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
            txn.commit();
            if (managerStatus == null || managerStatus.getStatus() != StatusType.STARTED) {
                return false;
            }
        } catch (Exception e) {
            log.error("Unable to acquire CompactionManager status, {}, {}", e, e.getStackTrace());
            return false;
        }
        return true;
    }

    public static CorfuStoreMetadata.TableName getTableName(Table<Message, Message, Message> table) {
        String fullName = table.getFullyQualifiedTableName();
        return CorfuStoreMetadata.TableName.newBuilder()
                .setNamespace(table.getNamespace())
                .setTableName(fullName.substring(fullName.indexOf("$") + 1))
                .build();
    }
}
