package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class CheckpointLivenessUpdater implements LivenessUpdater {
    private final CorfuStore corfuStore;
    private final ScheduledExecutorService executorService;

    private volatile Optional<TableName> currentTable = Optional.empty();

    private static final Duration UPDATE_INTERVAL = Duration.ofSeconds(8);

    private final Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointsTable;

    public CheckpointLivenessUpdater(CorfuStore corfuStore) {
        this.corfuStore = corfuStore;
        try {
            this.activeCheckpointsTable = getActiveCheckpointsTable();
        } catch (Exception e) {
            log.error("Opening ActiveCheckpointsTable failed due to exception: ", e);
            throw new IllegalThreadStateException("Opening ActiveCheckpointsTable failed " + e);
        }
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    private Table<TableName, ActiveCPStreamMsg, Message> getActiveCheckpointsTable()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        return this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME,
                TableName.class,
                ActiveCPStreamMsg.class,
                null,
                TableOptions.fromProtoSchema(ActiveCPStreamMsg.class));
    }

    @Override
    public void start() {
        executorService.scheduleWithFixedDelay(
                this::updateHeartbeat,
                UPDATE_INTERVAL.toMillis() / 2,
                UPDATE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    protected void updateHeartbeat() {
        TableName table;
        try {
            table = currentTable.get();
        } catch (NoSuchElementException e) {
            log.debug("Encountered NoSuchElementException while accessing currentTable, ", e);
            return;
        }

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            ActiveCPStreamMsg currentStatus = (ActiveCPStreamMsg)
                    txn.getRecord(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, table).getPayload();
            ActiveCPStreamMsg newStatus = ActiveCPStreamMsg.newBuilder()
                    .setSyncHeartbeat(currentStatus.getSyncHeartbeat() + 1)
                    .build();
            // update validity counter for the current table
            txn.putRecord(activeCheckpointsTable, table, newStatus, null);
            txn.commit();
        } catch (TransactionAbortedException ex) {
            if (ex.getAbortCause() == AbortCause.CONFLICT) {
                log.warn("Another thread tried to commit while updating heartbeat for table: {}, ", table, ex);
            }
        } catch (Exception e) {
            log.warn("Unable to update liveness for table: {} due to exception: {}", table, e.getStackTrace());
        } catch (Throwable t) {
            corfuStore.getRuntime().getParameters().getSystemDownHandler().run();
        }
    }

    @Override
    public void setCurrentTable(TableName tableName) {
        this.currentTable = Optional.of(tableName);
    }

    @Override
    public void unsetCurrentTable() {
        currentTable = Optional.empty();
    }

    @Override
    public void shutdown() {
        executorService.shutdownNow();
    }
}
