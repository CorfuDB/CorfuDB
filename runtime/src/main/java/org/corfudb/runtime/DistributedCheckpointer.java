package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.ICorfuTable;
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
    protected final CorfuStore corfuStore;
    private final LivenessUpdater livenessUpdater;
    private final CompactorMetadataTables compactorMetadataTables;
    private final CorfuRuntime corfuRuntime;
    private final String clientName;

    private long compactorCycleCount;

    public static final long CONN_RETRY_DELAY_MILLISEC = 500;
    public static final int MAX_RETRIES = 5;

    DistributedCheckpointer(@NonNull CorfuRuntime corfuRuntime, String clientName,
                            @NonNull CorfuStore corfuStore, CompactorMetadataTables compactorMetadataTables) {
        this.corfuRuntime = corfuRuntime;
        this.clientName = clientName;
        this.corfuStore = corfuStore;
        this.compactorMetadataTables = compactorMetadataTables;
        this.livenessUpdater = new CheckpointLivenessUpdater(corfuStore);
        this.livenessUpdater.start();
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
                    compactorCycleCount = tableStatus.getCycleCount();
                    txn.putRecord(compactorMetadataTables.getCheckpointingStatusTable(),
                            tableName,
                            CheckpointingStatus.newBuilder().setStatus(StatusType.STARTED).setClientName(clientName)
                                    .setCycleCount(compactorCycleCount).build(),
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
                    log.info("Table {}${} is being checkpointed by someone else",
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
                CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                        CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
                if (compactorCycleCount != managerStatus.getCycleCount() ||
                        managerStatus.getStatus() == StatusType.COMPLETED ||
                        managerStatus.getStatus() == StatusType.FAILED) {
                    log.error("Compaction cycle has already ended with status {}", managerStatus.getStatus());
                    txn.commit();
                    return false;
                }
                CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, tableName).getPayload();
                if (tableStatus.getStatus() == StatusType.FAILED) {
                    //Leader marked me as failed
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
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, MAX_RETRIES)) {
                    throw new IllegalStateException(re);
                }
            }
        }
        return false;
    }

    public boolean tryCheckpointTable(@NonNull TableName tableName, @NonNull Function<TableName,
            CheckpointWriter<ICorfuTable<?,?>>> checkpointWriterFn) {
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

    /**
     * Calls tryCheckpointTable for all tables opened by the current runtime
     */
    public void checkpointOpenedTables() {
        log.info("Checkpointing opened tables");
        for (Table<Message, Message, Message> openedTable : corfuRuntime.getTableRegistry().getAllOpenTables()) {
            boolean isSuccess = tryCheckpointTable(DistributedCheckpointerHelper.getTableName(openedTable),
                    t -> openedTable.getCheckpointWriter(corfuRuntime, "OpenedTableCheckpointer"));
            if (!isSuccess) {
                log.warn("Stop checkpointing after failure in {}", openedTable.getFullyQualifiedTableName());
                break;
            }
        }
    }

    private CheckpointingStatus appendCheckpoint(TableName tableName,
                                                 Function<TableName, CheckpointWriter<ICorfuTable<?,?>>> checkpointWriterFn) {
        long tableCkptStartTime = System.currentTimeMillis();
        log.info("{} Starting checkpoint: {}${}", clientName,
                tableName.getNamespace(), tableName.getTableName());

        this.livenessUpdater.setCurrentTable(tableName);
        StatusType returnStatus = StatusType.FAILED;
        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            CheckpointWriter<ICorfuTable<?,?>> cpw = null;
            try {
                cpw = checkpointWriterFn.apply(tableName);
                cpw.appendCheckpoint(Optional.of(livenessUpdater));
                returnStatus = StatusType.COMPLETED;
                break;
            } catch (RuntimeException re) {
                log.warn("Encountered RuntimeException, Message: {}, ", re.getMessage(), re);
                if (isCriticalRuntimeException(re, retry, MAX_RETRIES)) {
                    break; // stop on non-retryable exceptions
                }
            } catch (Exception ex) {
                log.warn("Encountered unexpected Exception, Message: {}, ", ex.getMessage(), ex);
            } finally {
                if (cpw != null) {
                    cpw.getCorfuTable().close();
                }
             }
        }
        this.livenessUpdater.unsetCurrentTable();
        return CheckpointingStatus.newBuilder()
                .setStatus(returnStatus).setClientName(clientName)
                .setCycleCount(compactorCycleCount).setTimeTaken(System.currentTimeMillis() - tableCkptStartTime)
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

    public void shutdown() {
        livenessUpdater.shutdown();
    }
}

