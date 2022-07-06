package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.proto.RpcCommon;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
@Getter
public class CompactorMetadataTables {
    private Table<StringKey, CheckpointingStatus, Message> compactionManagerTable;
    private Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable;
    private Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointsTable;
    private Table<StringKey, RpcCommon.TokenMsg, Message> checkpointTable;

    public static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManager";
    public static final String CHECKPOINT_STATUS_TABLE_NAME = "CheckpointStatusTable";
    public static final String ACTIVE_CHECKPOINTS_TABLE_NAME = "ActiveCheckpoints";
    public static final String CHECKPOINT = "checkpoint";

    public static final StringKey COMPACTION_MANAGER_KEY = StringKey.newBuilder().setKey("CompactionManagerKey").build();
    public static final StringKey CHECKPOINT_KEY = StringKey.newBuilder().setKey("minCheckpointToken").build();
    public static final StringKey UPGRADE_KEY = StringKey.newBuilder().setKey("UpgradeKey").build();
    public static final StringKey INSTANT_TIGGER_KEY = StringKey.newBuilder().setKey("InstantTrigger").build();

    private static final int MAX_RETRIES = 5;

    public CompactorMetadataTables(CorfuStore corfuStore) throws Exception {
        for (int retry = 0; ; retry++) {
            try {
                this.compactionManagerTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                        COMPACTION_MANAGER_TABLE_NAME,
                        StringKey.class,
                        CheckpointingStatus.class,
                        null,
                        TableOptions.fromProtoSchema(CheckpointingStatus.class));

                this.checkpointingStatusTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                        CHECKPOINT_STATUS_TABLE_NAME,
                        TableName.class,
                        CheckpointingStatus.class,
                        null,
                        TableOptions.fromProtoSchema(CheckpointingStatus.class));

                this.activeCheckpointsTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                        ACTIVE_CHECKPOINTS_TABLE_NAME,
                        TableName.class,
                        ActiveCPStreamMsg.class,
                        null,
                        TableOptions.fromProtoSchema(ActiveCPStreamMsg.class));

                this.checkpointTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                        CHECKPOINT,
                        StringKey.class,
                        RpcCommon.TokenMsg.class,
                        null,
                        TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));
                break;
            } catch (Exception e) {
                if (retry == MAX_RETRIES) {
                    throw e;
                }
                log.error("Caught an exception while opening Compaction metadata tables ", e);
            }
        }
    }
}
