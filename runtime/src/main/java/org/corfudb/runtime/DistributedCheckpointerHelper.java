package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;

import java.util.Date;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class DistributedCheckpointerHelper {
    public static boolean isCheckpointFrozen(CorfuStore corfuStore) { //TODO: needs change?
        final StringKey freezeCheckpointNS = StringKey.newBuilder().setKey("freezeCheckpointNS").build();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            RpcCommon.TokenMsg freezeToken = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.CHECKPOINT, freezeCheckpointNS).getPayload();

            final long patience = 2 * 60 * 60 * 1000;
            if (freezeToken != null) {
                long now = System.currentTimeMillis();
                long frozeAt = freezeToken.getSequence();
                Date frozeAtDate = new Date(frozeAt);
                if (now - frozeAt > patience) {
                    txn.delete(CompactorMetadataTables.CHECKPOINT, freezeCheckpointNS);
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

    public static boolean hasCompactionStarted(CorfuStore corfuStore) {
        if (isCheckpointFrozen(corfuStore)) { //This is necessary here to stop checkpointing after it has started
            return false;
        }
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            final CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
            txn.commit();
            if (managerStatus == null ||
                    managerStatus.getStatus() != StatusType.STARTED &&
                            managerStatus.getStatus() != StatusType.STARTED_ALL) {
                return false;
            }
        } catch (Exception e) {
            log.error("Unable to acquire CompactionManager status, {}, {}", e, e.getStackTrace());
            return false;
        }
        return true;
    }

    public static boolean isUpgrade(CorfuStore corfuStore) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            RpcCommon.TokenMsg upgradeToken = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.CHECKPOINT,
                    CompactorMetadataTables.UPGRADE_KEY).getPayload();
            txn.commit();
            if (upgradeToken != null) {
                log.warn("Client Checkpointer asked to freeze due to upgrade");
                return true;
            }
        }
        return false;
    }

    public static CorfuStoreMetadata.TableName getTableName(Table<Message, Message, Message> table) {
        String fullName = table.getFullyQualifiedTableName();
        return CorfuStoreMetadata.TableName.newBuilder()
                .setNamespace(table.getNamespace())
                .setTableName(fullName.substring(fullName.indexOf("$") + 1))
                .build();
    }
}
