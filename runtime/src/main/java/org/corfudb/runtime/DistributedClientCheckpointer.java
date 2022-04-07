package org.corfudb.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.proto.RpcCommon;

import javax.annotation.Nonnull;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.corfudb.runtime.CorfuCompactorManagement.*;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Runs the checkpointing of locally opened tables from within the client's JVM
 * <p>
 */
@Slf4j
public class DistributedClientCheckpointer {

    private final CorfuRuntime runtime;
    private final ScheduledExecutorService compactionScheduler;
    private CorfuStore corfuStore = null;
    private Table<StringKey, CheckpointingStatus, Message> compactionManagerTable = null;
    private Table<CorfuStoreMetadata.TableName, CheckpointingStatus, Message> checkpointingStatusTable = null;
    private Table<CorfuStoreMetadata.TableName, ActiveCPStreamMsg, Message> activeCheckpointsTable = null;
    private Table<StringKey, RpcCommon.TokenMsg, Message> checkpointFreezeTable = null;
    private Table<StringKey, RpcCommon.TokenMsg, Message> previousTokenTable = null;

    public DistributedClientCheckpointer(@Nonnull CorfuRuntime runtime) {
        this.runtime = runtime;
        if (runtime.getParameters().checkpointTriggerFreqSecs > 0) {
            this.compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat(runtime.getParameters().getClientName() + "-chkpter")
                            .build());
            compactionScheduler.scheduleAtFixedRate(this::runClientCheckpoints,
                    runtime.getParameters().getCheckpointTriggerFreqSecs()*2,
                    runtime.getParameters().getCheckpointTriggerFreqSecs(),
                    TimeUnit.SECONDS
            );
        } else {
            compactionScheduler = null;
        }
    }

    private void tryInitClientCheckpointing() {
        try {
            if (this.corfuStore != null) { // only run this method once
                return;
            }
            this.corfuStore = new CorfuStore(this.runtime);
            log.debug("Opening all the checkpoint metadata tables");
            this.compactionManagerTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    StringKey.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));

            this.checkpointingStatusTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT_STATUS_TABLE_NAME,
                    CorfuStoreMetadata.TableName.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));

            this.activeCheckpointsTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.ACTIVE_CHECKPOINTS_TABLE_NAME,
                    CorfuStoreMetadata.TableName.class,
                    ActiveCPStreamMsg.class,
                    null,
                    TableOptions.fromProtoSchema(ActiveCPStreamMsg.class));

            this.checkpointFreezeTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT,
                    StringKey.class,
                    RpcCommon.TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));

            this.previousTokenTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.PREVIOUS_TOKEN,
                    StringKey.class,
                    RpcCommon.TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));
        } catch (Exception e) {
            log.error("Caught an exception while opening checkpoint management tables ", e);
        }
    }

    private synchronized void runClientCheckpoints() {
        tryInitClientCheckpointing();

        if (!checkGlobalCheckpointStartTrigger()) {
            return;
        }

        for (Table<Message, Message, Message> openedTable :
                this.runtime.getTableRegistry().getAllOpenTablesForCheckpointing()) {
            tryCheckpointTable(openedTable);
        }
    }

    private boolean checkGlobalCheckpointStartTrigger() {
        // This is necessary here to stop checkpointing after it has started?
        // if (isCheckpointFrozen(corfuStore, this.checkpointFreezeTable)) {return;}
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            final CorfuStoreEntry<StringKey, CheckpointingStatus, Message> compactionStatus =
                    txn.getRecord(compactionManagerTable, DistributedCompactor.COMPACTION_MANAGER_KEY);
            if (compactionStatus.getPayload() == null ||
                    compactionStatus.getPayload().getStatus() != CheckpointingStatus.StatusType.STARTED) {
                txn.commit();
                return false;
            }
            txn.commit();
        } catch (Exception e) {
            log.error("Checkpointer unable to check the main status table", e);
            return false;
        }
        return true;
    }

    /**
     * Distributed Checkpointing involves 3 steps:
     * 1. Acquire distributed lock on the table to be checkpointed using transactions.
     * 2. Attempt to checkpoint the table, retry on retryable errors like WrongEpochException.
     * 3. If successful, unlock the table. if unsuccessful mark the checkpoint as failed.
     * @param myTable - the locally opened Table instance to be checkpointed
     */
    private void tryCheckpointTable(Table<Message, Message, Message> myTable) {
        CorfuStoreMetadata.TableName tableName = getTableName(myTable);
        if (!tryLockMyTableToCheckpoint(myTable, tableName)) {
            return;
        }

        Token endToken = tryCheckpointMyTable(myTable);

        unlockMyCheckpointTable(tableName, endToken);
    }

    /**
     * @param myTable - the instance we want to lock on
     * @param tableName - protobuf name of the table used as key for the granular lock
     * @return true if the table can be checkpointed by me
     *          false if lock acquisition fails due to race or a different error
     */
    private boolean tryLockMyTableToCheckpoint(Table<Message, Message, Message> myTable,
                                               CorfuStoreMetadata.TableName tableName) {
        final int maxRetries = 5;
        for (int retry = 0; retry < maxRetries; retry++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                final CorfuStoreEntry<CorfuStoreMetadata.TableName, CheckpointingStatus, Message> tableToChkpt =
                        txn.getRecord(checkpointingStatusTable, tableName);
                if (tableToChkpt.getPayload().getStatus() == CheckpointingStatus.StatusType.IDLE) {
                    txn.putRecord(checkpointingStatusTable,
                            tableName,
                            CheckpointingStatus.newBuilder()
                                    .setStatus(CheckpointingStatus.StatusType.STARTED)
                                    .setClientId(RpcCommon.UuidMsg.newBuilder()
                                            .setLsb(runtime.getParameters().getClientId().getLeastSignificantBits())
                                            .setMsb(runtime.getParameters().getClientId().getMostSignificantBits())
                                            .build())
                                    .build(),
                            null);
                    txn.putRecord(activeCheckpointsTable, tableName,
                            ActiveCPStreamMsg.newBuilder()
                                    .setIsClientTriggered(true) // This will stall server side compaction!
                                    .build(),
                            null);
                } else { // This table is already being checkpointed by someone else
                    txn.commit();
                    return false;
                }
                txn.commit();
                return true; // Lock successfully acquired!
            } catch (TransactionAbortedException e) {
                if (e.getAbortCause() == AbortCause.CONFLICT) {
                    log.info("My opened table {} is being checkpointed by someone else",
                            myTable.getFullyQualifiedTableName());
                    return false;
                }
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, maxRetries)) {
                    return false; // stop on non-retryable exceptions
                }
            } catch (Throwable t) {
                log.error("clientCheckpointer: encountered unexpected exception", t);
                log.error("StackTrace: {}", t.getStackTrace());
            }
        }
        return false;
    }

    /**
     * This is the routine where the actual checkpointing is invoked
     * @param myTable - the table to be checkpointed
     * @return the token returned from the checkpointing, null if failure happens
     */
    private Token tryCheckpointMyTable(Table<Message, Message, Message> myTable) {
        final int maxRetries = 5;
        for (int retry = 0; retry < maxRetries; retry++) {
            try {
                log.info("Client checkpointing locally opened table {}", myTable.getFullyQualifiedTableName());
                return DistributedCompactor.appendCheckpoint(myTable.getCorfuTableForCheckpointingOnly(),
                        getTableName(myTable), runtime);
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, maxRetries)) {
                    return null; // stop on non-retryable exceptions
                }
            } catch (Throwable t) {
                log.error("clientCheckpointer: encountered unexpected exception", t);
                log.error("StackTrace: {}", t.getStackTrace());
            }
        }
        return null;
    }

    /**
     * Mark the checkpointed table as either done or failed based on endToken
     * @param tableName - protobuf name of the table just checkpointed
     * @param endToken - final token at which the checkpoint snapshot was written
     */
    private void unlockMyCheckpointTable(CorfuStoreMetadata.TableName tableName, Token endToken) {
        final int maxRetries = 5;
        for (int retry = 0; retry < maxRetries; retry++) {
            try (TxnContext endTxn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                if (endToken == null) {
                    log.error("clientCheckpointer: Marking checkpointing as failed on table {}", tableName);
                    endTxn.putRecord(checkpointingStatusTable,
                            tableName,
                            CheckpointingStatus.newBuilder()
                                    .setStatus(CheckpointingStatus.StatusType.FAILED)
                                    .setClientId(RpcCommon.UuidMsg.newBuilder()
                                            .setLsb(runtime.getParameters().getClientId().getLeastSignificantBits())
                                            .setMsb(runtime.getParameters().getClientId().getMostSignificantBits())
                                            .build())
                                    .build(),
                            null);
                } else {
                    endTxn.putRecord(checkpointingStatusTable,
                            tableName,
                            CheckpointingStatus.newBuilder()
                                    .setStatus(CheckpointingStatus.StatusType.COMPLETED)
                                    .setClientId(RpcCommon.UuidMsg.newBuilder()
                                            .setLsb(runtime.getParameters().getClientId().getLeastSignificantBits())
                                            .setMsb(runtime.getParameters().getClientId().getMostSignificantBits())
                                            .build())
                                    .setEndToken(RpcCommon.TokenMsg.newBuilder()
                                            .setEpoch(endToken.getEpoch())
                                            .setSequence(endToken.getSequence())
                                            .build())
                                    .build(),
                            null);
                }
                endTxn.delete(activeCheckpointsTable, tableName);
                endTxn.commit();
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, maxRetries)) {
                    return; // stop on non-retryable exceptions
                }
            } catch (Throwable t) {
                log.error("clientCheckpointer: encountered unexpected exception", t);
                log.error("StackTrace: {}", t.getStackTrace());
            }
        }
    }

    /**
     *
     * @param re - the exception this method is called on
     * @param retry - the number of times retries have been done
     * @param maxRetries - max number of times retries need to happen
     * @return - True if we should stop & return. False if we can retry!
     */
    private boolean isCriticalRuntimeException(RuntimeException re, int retry, int maxRetries) {
        log.trace("checkpointer: encountered an exception on attempt {}/{}.",
                retry, maxRetries, re);

        if (retry == maxRetries - 1) {
            log.error("checkpointer: retry exhausted.", re);
            return true;
        }

        if (re instanceof NetworkException || re.getCause() instanceof TimeoutException) {
            try {
                TimeUnit.MILLISECONDS.sleep(DistributedCompactor.CONN_RETRY_DELAY_MILLISEC);
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

    public static CorfuStoreMetadata.TableName getTableName(Table<Message, Message, Message> table) {
        String fullName = table.getFullyQualifiedTableName();
        return CorfuStoreMetadata.TableName.newBuilder()
                .setNamespace(table.getNamespace())
                .setTableName(fullName.substring(fullName.indexOf("$")+1))
                .build();
    }

    /**
     * Shutdown the streaming manager and clean up resources.
     */
    public synchronized void shutdown() {
        if (compactionScheduler != null) {
            this.compactionScheduler.shutdown();
        }
    }
}
