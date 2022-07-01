package org.corfudb.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WrongClusterException;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public abstract class DistributedCheckpointer {
    public static final long CONN_RETRY_DELAY_MILLISEC = 500;
    public static final int MAX_RETRIES = 5;

    private Optional<CorfuStore> corfuStore;
    private Optional<LivenessUpdater> livenessUpdater;
    protected CompactorMetadataTables compactorMetadataTables = null;

    private final CorfuRuntime corfuRuntime;
    private final String clientName;

    private long epoch;

    DistributedCheckpointer(@NonNull CorfuRuntime corfuRuntime, String clientName) {
        this.corfuRuntime = corfuRuntime;
        this.clientName = clientName;
        this.corfuStore = Optional.empty();
        this.livenessUpdater = Optional.empty();
    }

    @VisibleForTesting
    public CorfuStore getCorfuStore() {
        if (!this.corfuStore.isPresent()) {
            this.corfuStore = Optional.of(new CorfuStore(this.corfuRuntime));
        }
        return this.corfuStore.get();
    }

    @VisibleForTesting
    public LivenessUpdater getLivenessUpdater() {
        if (!livenessUpdater.isPresent()) {
            this.livenessUpdater = Optional.of(new CheckpointLivenessUpdater(getCorfuStore()));
        }
        return this.livenessUpdater.get();
    }

    @VisibleForTesting
    public boolean openCompactorMetadataTables() {
        log.debug("Open all checkpoint metadata tables");
        try {
            compactorMetadataTables = new CompactorMetadataTables(getCorfuStore());
        } catch (Exception e) {
            log.error("Exception while opening compactorMetadataTables, ", e);
            return false;
        }
        return true;
    }

    private boolean tryLockTableToCheckpoint(@NonNull CompactorMetadataTables compactorMetadataTables,
                                             @NonNull TableName tableName) throws RuntimeException {
        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            try (TxnContext txn = getCorfuStore().txn(CORFU_SYSTEM_NAMESPACE)) {
                CorfuStoreEntry<TableName, CheckpointingStatus, Message> tableToChkpt = txn.getRecord(
                        CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, tableName);
                if (tableToChkpt.getPayload().getStatus() == StatusType.IDLE) {
                    epoch = tableToChkpt.getPayload().getEpoch();
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
                    throw re;
                }
            }
        }
        return false;
    }

    private boolean unlockTableAfterCheckpoint(@NonNull CompactorMetadataTables compactorMetadataTables,
                                               @NonNull TableName tableName,
                                               @NonNull CheckpointingStatus checkpointStatus) throws RuntimeException {
        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            try (TxnContext txn = getCorfuStore().txn(CORFU_SYSTEM_NAMESPACE)) {
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
                    throw re;
                }
            }
        }
        return false;
    }

    public boolean tryCheckpointTable(TableName tableName, Function<TableName, CorfuTable> openTableFn) {
        try {
            if (!tryLockTableToCheckpoint(compactorMetadataTables, tableName)) {
                // Failure to get a lock is treated as success
                return true;
            }
            CheckpointingStatus checkpointStatus = appendCheckpoint(openTableFn.apply(tableName), tableName, new MultiCheckpointWriter());
            return unlockTableAfterCheckpoint(compactorMetadataTables, tableName, checkpointStatus);
        } catch (RuntimeException re) {
            return false;
        }
    }

    @AllArgsConstructor
    @Getter
    public static class CorfuTableNamePair {
        private TableName tableName;
        private CorfuTable corfuTable;
    }

    public int checkpointOpenedTables() {
        int count = 0;
        for (CorfuTableNamePair openedTable :
                corfuRuntime.getTableRegistry().getAllOpenTablesForCheckpointing()) {

            boolean isSuccess = tryCheckpointTable(openedTable.tableName, t -> openedTable.corfuTable);
            if (!isSuccess) {
                log.warn("Stop checkpointing after failure in {}${}",
                        openedTable.tableName.getNamespace(), openedTable.tableName.getTableName());
                break;
            }
            count++;
        }
        return count;
    }

    private <K, V> CheckpointingStatus appendCheckpoint(CorfuTable<K, V> corfuTable,
                                                        TableName tableName,
                                                        MultiCheckpointWriter<CorfuTable> mcw) {
        mcw.addMap(corfuTable);
        long tableCkptStartTime = System.currentTimeMillis();
        log.info("{} Starting checkpoint: {}${}", clientName,
                tableName.getNamespace(), tableName.getTableName());

        getLivenessUpdater().updateLiveness(tableName);
        StatusType returnStatus = StatusType.FAILED;
        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            try {
                mcw.appendCheckpoints(corfuRuntime, "checkpointer", this.livenessUpdater);
                returnStatus = StatusType.COMPLETED;
                break;
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, MAX_RETRIES)) {
                    break; // stop on non-retryable exceptions
                }
            } catch (Exception e) {
                log.error("Unable to checkpoint table: {}, e: {}", tableName, e);
            }
        }
        getLivenessUpdater().notifyOnSyncComplete();
        return CheckpointingStatus.newBuilder()
                .setStatus(returnStatus).setClientName(clientName)
                .setTableSize(corfuTable.size()).setTimeTaken(System.currentTimeMillis() - tableCkptStartTime)
                .setEpoch(epoch).build();
    }

    protected boolean isCriticalRuntimeException(RuntimeException re, int retry, int maxRetries) {
        log.trace("checkpointer: encountered an exception on attempt {}/{}.",
                retry, maxRetries, re);

        if (retry == maxRetries - 1) {
            log.error("checkpointer: retry exhausted.", re);
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

        if (re instanceof WrongClusterException) {
            log.error("Wrong cluster exception hit! stopping right away!");
            return true;
        }
        return false; // it is ok to retry a few times on network timeouts
    }

    public abstract void checkpointTables();
}

