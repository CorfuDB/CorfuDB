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
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.proto.RpcCommon;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class DistributedCheckpointerHelper {

    private final CorfuStore corfuStore;

    @Getter
    private final CompactorMetadataTables compactorMetadataTables;

    private static final long COMPACTION_TRIGGER_INTERVAL = 10;

    public DistributedCheckpointerHelper(CorfuStore corfuStore) throws Exception {
        this.corfuStore = corfuStore;

        // Open all compactor related tables
        this.compactorMetadataTables = new CompactorMetadataTables(corfuStore);
    }

    public static enum UpdateAction {
        PUT,
        DELETE
    }

    public boolean disableCompaction() {
        return updateCompactionControlsTable(
                CompactorMetadataTables.DISABLE_COMPACTION, UpdateAction.PUT);
    }

    public void disableCompactionWithWait() {
        boolean isCompactionInProgress = disableCompaction();

        if (isCompactionInProgress) {
            // Wait for time longer than Compactor's orchestrator scheduling period to ensure
            // the disable command is effective across the cluster
            try {
                log.info("Waiting ({} seconds) for the disabling to complete...", COMPACTION_TRIGGER_INTERVAL);
                TimeUnit.SECONDS.sleep(2 * COMPACTION_TRIGGER_INTERVAL);
                log.info("Done waiting for {}", COMPACTION_TRIGGER_INTERVAL);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(ie);
            }
        } else {
            log.info("Compaction is not started at the time of disabling. Skip waiting.");
        }

        log.info("Disabled compaction.");
    }

    public void enableCompaction() {
        updateCompactionControlsTable(CompactorMetadataTables.DISABLE_COMPACTION, UpdateAction.DELETE);
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

    /**
     * Update the CompactionControlsTable with given Key and Action.
     * Return if compaction is STARTED at the time of the update.
     *
     * @param stringKey the control operation in StringKey format
     * @param action the action to perform
     * @return if compaction is STARTED at the time of the update
     */
    private boolean updateCompactionControlsTable(StringKey stringKey, UpdateAction action) {
        log.info("Updating CompactionControlsTable with Key:{}, Action:{}", stringKey, action);

        boolean isCompactionInProgress = false;
        Table<StringKey, RpcCommon.TokenMsg, Message> compactionControlsTable =
                compactorMetadataTables.getCompactionControlsTable();

        for (int retry = 1; retry <= CompactorMetadataTables.MAX_RETRIES; retry++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                updateCompactionControlsTable(txn, compactionControlsTable, stringKey, action);

                CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                        CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
                isCompactionInProgress = (managerStatus != null && managerStatus.getStatus().equals(StatusType.STARTED));
                log.info("During updateCompactionControlsTable, isCompactionInProgress? {}", isCompactionInProgress);

                txn.commit();
                break;
            } catch (TransactionAbortedException tae) {
                if (tae.getAbortCause() == AbortCause.CONFLICT) {
                    log.error("Another transaction has updated the CompactionControlsTable while " +
                            "updating with Key:{}, Action:{}. Abort retrying.", stringKey, action);
                    throw tae;
                }
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
        log.info("Updated CompactionControlsTable with Key:{}, Action:{}", stringKey, action);
        return isCompactionInProgress;
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
