package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;

import java.util.Date;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class DistributedCheckpointerHelper {

    private final CorfuStore corfuStore;

    public DistributedCheckpointerHelper(CorfuStore corfuStore) {
        this.corfuStore = corfuStore;
    }

    public static enum UpdateAction {
        PUT,
        DELETE
    }

    public void updateCompactionControlsTable(Table<StringKey, RpcCommon.TokenMsg, Message> checkpointTable,
                                              StringKey stringKey,
                                              UpdateAction action) {

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            updateCompactionControlsTable(txn, checkpointTable, stringKey, action);
            txn.commit();
        } catch (Exception e) {
            log.warn("Unable to write UpgradeKey to checkpoint table, ", e);
        }
    }
    public void updateCompactionControlsTable(TxnContext txn,
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
