package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.WrongEpochException;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public abstract class DistributedCheckpointer {
    public static final long CONN_RETRY_DELAY_MILLISEC = 500;
    public static final int MAX_RETRIES = 5;

    protected final CorfuStore corfuStore;
    private final LivenessUpdater livenessUpdater;
    protected CompactorMetadataTables compactorMetadataTables = null;

    private final CorfuRuntime corfuRuntime;
    private final String clientName;

    private long epoch;

    DistributedCheckpointer(@NonNull CorfuRuntime corfuRuntime, String clientName,
                            CorfuStore corfuStore, CompactorMetadataTables compactorMetadataTables) {
        this.corfuRuntime = corfuRuntime;
        this.clientName = clientName;
        this.corfuStore = corfuStore;
        this.compactorMetadataTables = compactorMetadataTables;
        this.livenessUpdater = new CheckpointLivenessUpdater(corfuStore);
    }

    private boolean tryLockTableToCheckpoint(@NonNull CompactorMetadataTables compactorMetadataTables,
                                             @NonNull TableName tableName) throws IllegalStateException {
        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                        CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
                if (managerStatus.getStatus() != StatusType.STARTED) {
                    txn.commit();
                    throw new IllegalStateException("Compaction has not started. Stop checkpointing");
                }
                CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, tableName).getPayload();
                if (tableStatus.getStatus() == StatusType.IDLE) {
                    epoch = tableStatus.getEpoch();
                    txn.putRecord(compactorMetadataTables.getCheckpointingStatusTable(),
                            tableName,
                            CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).setClientName(clientName)
                                    .setEpoch(epoch).build(),
                            null);
                    txn.putRecord(compactorMetadataTables.getActiveCheckpointsTable(), tableName,
                            CorfuCompactorManagement.ActiveCPStreamMsg.newBuilder().build(),
                            null);
                    txn.commit();
                    return true; // Lock successfully acquired!
                }
                txn.commit();
                break;
            } catch (TransactionAbortedException e) {
                if (e.getAbortCause() == AbortCause.CONFLICT) {
                    log.info("My opened table {}${} is being checkpointed by someone else",
                            tableName.getNamespace(), tableName.getTableName());
                    return false;
                }
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, MAX_RETRIES)) {
                    throw new IllegalStateException(re);
                }
            }
        }
        return false;
    }

    private boolean unlockTableAfterCheckpoint(@NonNull CompactorMetadataTables compactorMetadataTables,
                                               @NonNull TableName tableName,
                                               @NonNull CheckpointingStatus checkpointStatus) throws IllegalStateException {
        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                CorfuStoreEntry<StringKey, CheckpointingStatus, Message> managerStatus = txn.getRecord(
                        CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                        CompactorMetadataTables.COMPACTION_MANAGER_KEY);
                if (epoch != managerStatus.getPayload().getEpoch() ||
                        managerStatus.getPayload().getStatus() == StatusType.COMPLETED ||
                        managerStatus.getPayload().getStatus() == StatusType.FAILED) {
                    log.error("Compaction cycle has already ended with status {}", managerStatus.getPayload().getStatus());
                    txn.commit();
                    return false;
                }
                CorfuStoreEntry<TableName, CheckpointingStatus, Message> tableStatus = txn.getRecord(
                        CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, tableName);
                if (tableStatus.getPayload().getStatus() == StatusType.FAILED) {
                    log.error("Table status for {}${} has already been marked as FAILED",
                            tableName.getNamespace(), tableName.getTableName());
                    txn.commit();
                    return false;
                }
                txn.putRecord(compactorMetadataTables.getCheckpointingStatusTable(), tableName, checkpointStatus,
                        null);
                txn.delete(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, tableName);
                txn.commit();
                return true;
            } catch (TransactionAbortedException e) {
                log.error("TransactionAbortedException exception while trying to unlock table {}${}: {}",
                        tableName.getNamespace(), tableName.getTableName(), e.getMessage());
                break; //Leader marked me as failed
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, MAX_RETRIES)) {
                    throw new IllegalStateException(re);
                }
            }
        }
        return false;
    }

    public boolean tryCheckpointTable(@NonNull TableName tableName, @NonNull Function<TableName,
            CheckpointWriter<StreamingMap>> checkpointWriterFn) {
        try {
            if (!tryLockTableToCheckpoint(compactorMetadataTables, tableName)) {
                // Failure to get a lock is treated as success
                return true;
            }
            CheckpointingStatus checkpointStatus = appendCheckpoint(tableName, checkpointWriterFn);
            return unlockTableAfterCheckpoint(compactorMetadataTables, tableName, checkpointStatus);
        } catch (IllegalStateException e) {
            log.warn("TryCheckpointTable caught an exception: ", e);
            return false;
        }
    }

    public void checkpointOpenedTables() {
        log.info("Checkpointing opened tables");
        for (Table<Message, Message, Message> openedTable : corfuRuntime.getTableRegistry().getAllOpenTables()) {
            boolean isSuccess = tryCheckpointTable(DistributedCheckpointerHelper.getTableName(openedTable),
                    t -> openedTable.getCheckpointWriter(corfuRuntime, "DistributedCheckpointer"));
            if (!isSuccess) {
                log.warn("Stop checkpointing after failure in {}", openedTable.getFullyQualifiedTableName());
                break;
            }
        }
    }

    private CheckpointingStatus appendCheckpoint(TableName tableName,
                                                 Function<TableName, CheckpointWriter<StreamingMap>> checkpointWriterFn) {
        long tableCkptStartTime = System.currentTimeMillis();
        log.info("{} Starting checkpoint: {}${}", clientName,
                tableName.getNamespace(), tableName.getTableName());

        this.livenessUpdater.updateLiveness(tableName);
        StatusType returnStatus = StatusType.FAILED;
        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            try {
                CheckpointWriter<StreamingMap> cpw = checkpointWriterFn.apply(tableName);
                cpw.appendCheckpoint(Optional.of(livenessUpdater));
                returnStatus = StatusType.COMPLETED;
                break;
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, MAX_RETRIES)) {
                    break; // stop on non-retryable exceptions
                }
            }
        }
        this.livenessUpdater.notifyOnSyncComplete();
        return CheckpointingStatus.newBuilder()
                .setStatus(returnStatus).setClientName(clientName)
                .setEpoch(epoch).setTimeTaken(System.currentTimeMillis() - tableCkptStartTime)
                .build();
    }

    public static boolean isCriticalRuntimeException(RuntimeException re, int retry, int maxRetries) {
        log.trace("Encountered an exception on attempt {}/{}.",
                retry, maxRetries, re);

        if (retry == maxRetries - 1) {
            log.error("Retry exhausted.", re);
            return true;
        }

        if (re instanceof NetworkException || re.getCause() instanceof TimeoutException) {
            try {
                TimeUnit.MILLISECONDS.sleep(CONN_RETRY_DELAY_MILLISEC);
            } catch (InterruptedException e) {
                log.error("Interrupted in network retry sleep");
                return true;
            }
        }
        if (re instanceof WrongEpochException) {
            log.info("Epoch changed to {}. Sequencer failover can lead to potential epoch regression, retry {}/{}",
                    ((WrongEpochException) re).getCorrectEpoch(), retry, MAX_RETRIES);
        }

        if (re instanceof WrongClusterException) {
            log.error("Wrong cluster exception hit! stopping right away!");
            return true;
        }
        return false; // it is ok to retry a few times on network timeouts
    }

    public abstract void checkpointTables();
}

