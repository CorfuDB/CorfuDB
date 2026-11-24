package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CompactionCycleHistory;
import org.corfudb.runtime.CorfuCompactorManagement.CompactionCycleKey;
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
    private Table<StringKey, RpcCommon.TokenMsg, Message> compactionControlsTable;
    private Table<CompactionCycleKey, CompactionCycleHistory, Message> compactionCycleHistoryTable;
    public static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManagerTable";
    public static final String CHECKPOINT_STATUS_TABLE_NAME = "CheckpointStatusTable";
    public static final String ACTIVE_CHECKPOINTS_TABLE_NAME = "ActiveCheckpointsTable";
    public static final String COMPACTION_CONTROLS_TABLE = "CompactionControlsTable";
    public static final String COMPACTION_CYCLE_HISTORY_TABLE_NAME = "CompactionCycleHistoryTable";

    public static final StringKey COMPACTION_MANAGER_KEY = StringKey.newBuilder().setKey("CompactionManagerKey").build();
    public static final StringKey MIN_CHECKPOINT = StringKey.newBuilder().setKey("MinCheckpointToken").build();
    public static final StringKey FREEZE_TOKEN = StringKey.newBuilder().setKey("freezeCheckpointNS").build();
    public static final StringKey INSTANT_TIGGER = StringKey.newBuilder().setKey("InstantTrigger").build();
    public static final StringKey DISABLE_COMPACTION = StringKey.newBuilder().setKey("DisableCompaction").build();
    public static final StringKey INSTANT_TIGGER_WITH_TRIM = StringKey.newBuilder().setKey("InstantTriggerTrim").build();

    public static final int MAX_RETRIES = 5;

    public static final int TABLE_UPDATE_RETRY_SLEEP_SECONDS = 2;

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

                this.compactionControlsTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                        COMPACTION_CONTROLS_TABLE,
                        StringKey.class,
                        RpcCommon.TokenMsg.class,
                        null,
                        TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));

                this.compactionCycleHistoryTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                        COMPACTION_CYCLE_HISTORY_TABLE_NAME,
                        CompactionCycleKey.class,
                        CompactionCycleHistory.class,
                        null,
                        TableOptions.fromProtoSchema(CompactionCycleHistory.class));
                break;
            } catch (Exception e) {
                if (retry == MAX_RETRIES) {
                    log.error("Failed to open Compactor metadata tables after retry for {} times", MAX_RETRIES);
                    throw e;
                }
                log.warn("Caught an exception while opening Compaction metadata tables. Retrying...", e);
            }
        }
    }
}
