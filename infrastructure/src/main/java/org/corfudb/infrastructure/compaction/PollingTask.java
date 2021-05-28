package org.corfudb.infrastructure.compaction;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.infrastructure.compaction.proto.LogCompaction.CheckpointTaskMsg;
import org.corfudb.infrastructure.compaction.proto.LogCompaction.CheckpointTaskStatus;
import org.corfudb.infrastructure.compaction.proto.LogCompaction.PollingTaskTriggerKeyMsg;
import org.corfudb.infrastructure.compaction.proto.LogCompaction.PollingTaskTriggerMsg;
import org.corfudb.infrastructure.compaction.proto.LogCompaction.StatusKeyMsg;
import org.corfudb.infrastructure.compaction.proto.LogCompaction.StatusMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuQueue;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.corfudb.infrastructure.compaction.CompactionManager.COMPACTION_NAMESPACE;
import static org.corfudb.infrastructure.compaction.CompactionManager.COMPACTION_STREAM_TAG;
import static org.corfudb.infrastructure.compaction.CompactionManager.TRIGGER_TABLE_NAME;

@Slf4j
public class PollingTask implements Runnable, StreamListener {
    public static final int STATUS_RETRY_NUM = 5;
    public static final Duration STATUS_RETRY_MAX_DURATION = Duration.ofSeconds(3);
    public static final String EXCEPTION_MESSAGE = "Task Status failed to report";

    private final CorfuStore corfuStore;

    private final Table<StatusKeyMsg, StatusMsg, Message> statusTable;

    private final CorfuQueue checkpointTaskQueue;

    private final CheckpointTaskProcessor taskProcessor;

    // The Thread pool for executing polling tasks.
    private final ScheduledExecutorService pollingExecutor;

    private volatile SafeSnapshot safeSnapshot;

    private volatile String compactionTaskId;

    public PollingTask(CorfuRuntime runtime, CorfuStore corfuStore, String diskModePath,
                       CorfuQueue checkpointTaskQueue,
                       Table<StatusKeyMsg, StatusMsg, Message> statusTable,
                       ScheduledExecutorService pollingExecutor) {
        this.corfuStore = corfuStore;
        this.checkpointTaskQueue = checkpointTaskQueue;
        this.statusTable = statusTable;

        this.pollingExecutor = pollingExecutor;
        this.taskProcessor = new CheckpointTaskProcessor(diskModePath, runtime);

        this.safeSnapshot = null;
        this.compactionTaskId = null;
    }

    @Override
    public void run() {
        try {
            pollTaskQueue();
        } catch (Exception e) {
            log.error("Polling task stopped! It will restart if the next trigger msg arrives!", e);
        }
    }

    private void pollTaskQueue() {
        ByteString taskEntryByteString = checkpointTaskQueue.poll();
        if (taskEntryByteString == null) {
            log.warn("CheckpointTaskQueue is empty! Will wait for the next compaction cycle!");
        }

        try {
            CheckpointTaskMsg checkpointTaskMsg = CheckpointTaskMsg.parseFrom(taskEntryByteString);

            // Verify compaction task id.
            if (!checkpointTaskMsg.getCompactionTaskId().equals(compactionTaskId)) {
                log.warn("Checkpoint task {} is not in the current cycle {}!",
                        checkpointTaskMsg.toString(), compactionTaskId);

                // Ignore this task, pick up the next one.
                pollingExecutor.submit(this);
                return;
            }

            CheckpointOptions options = CheckpointOptions.builder()
                    .isDiskBacked(checkpointTaskMsg.getIsDiskBased())
                    .build();
            CheckpointTaskRequest request = CheckpointTaskRequest.builder()
                    .tableName(checkpointTaskMsg.getTableName())
                    .namespace(checkpointTaskMsg.getNamespace())
                    .safeSnapshot(safeSnapshot)
                    .options(options)
                    .build();

            // Report task assigned.
            reportTaskAssigned(request);

            // Process and report.
            CheckpointTaskResponse response = MicroMeterUtils.time(() ->
                    taskProcessor.executeCheckpointTask(request),
                    "compaction.checkpoint.task.timer", "type", "all_stream");

            handleResponse(response);
        } catch (InvalidProtocolBufferException ie) {
            log.error("Parse checkpoint task failed!", ie);
        } catch (RetryExhaustedException re) {
            log.warn("Task status report retry is exhausted! ", re);
        }

        // Re-submit itself to the executor so polling will start again.
        pollingExecutor.submit(this);
    }

    @Override
    public void onNext(CorfuStreamEntries results) {
        // update compaction id and safe snapshot
        results.getEntries().forEach((k, v) -> v.forEach(entry -> {
            PollingTaskTriggerKeyMsg keyMsg = (PollingTaskTriggerKeyMsg) entry.getKey();
            PollingTaskTriggerMsg triggerMsg = (PollingTaskTriggerMsg) entry.getPayload();

            compactionTaskId = keyMsg.getCompactionTaskId();
            safeSnapshot = SafeSnapshot.fromMessage(triggerMsg.getSafeSnapshot());
            log.info("Received a trigger msg with task id {}, status {}, and safe snapshot {}",
                    compactionTaskId, triggerMsg.getTaskStatus(), safeSnapshot);
        }));

        // start poll task for the incoming cycle
        pollingExecutor.submit(this);
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Trigger Listener hit an error, will subscribe again.", throwable);
        corfuStore.subscribeListener(this, COMPACTION_NAMESPACE, COMPACTION_STREAM_TAG,
                Collections.singletonList(TRIGGER_TABLE_NAME), null);
    }

    private void handleResponse(CheckpointTaskResponse response) {
        CheckpointTaskRequest request = response.getRequest();
        StatusKeyMsg keyMsg = StatusKeyMsg.newBuilder()
                .setCompactionTaskId(request.getCompactionTaskId())
                .setNamespace(request.getNamespace())
                .setTableName(request.getTableName())
                .build();
        StatusMsg statusMsg = null;
        if (response.getStatus() == CheckpointTaskResponse.Status.FINISHED) {
            // task finished
            statusMsg = StatusMsg.newBuilder()
                    .setStatus(CheckpointTaskStatus.FINISHED)
                    .build();
        } else {
            // task failed
            statusMsg = StatusMsg.newBuilder()
                    .setStatus(CheckpointTaskStatus.FAILED)
                    .setDetailMsg(response.getCauseOfFailure()
                            .map(Throwable::getMessage)
                            .orElse("Cause of failure is missing"))
                    .build();
        }

        try {
            updateStatusWithRetry(keyMsg, statusMsg);
        } catch (InterruptedException ie) {
            log.error("Unrecoverable exception when attempting to update status.", ie);
            Thread.currentThread().interrupt();
        }
    }

    private void reportTaskAssigned(CheckpointTaskRequest request) {

        StatusKeyMsg keyMsg = StatusKeyMsg.newBuilder()
                .setCompactionTaskId(request.getCompactionTaskId())
                .setNamespace(request.getNamespace())
                .setTableName(request.getTableName())
                .build();

        StatusMsg statusMsg = StatusMsg.newBuilder()
                .setStatus(CheckpointTaskStatus.ASSIGNED)
                .build();

        try {
            updateStatusWithRetry(keyMsg, statusMsg);
        } catch (InterruptedException ie) {
            log.error("Unrecoverable exception when attempting to update status.", ie);
            Thread.currentThread().interrupt();
        }
    }


    private void updateStatusWithRetry(StatusKeyMsg keyMsg, StatusMsg statusMsg)
            throws InterruptedException {
        AtomicInteger retryCount = new AtomicInteger(STATUS_RETRY_NUM);
        IRetry.build(ExponentialBackoffRetry.class, () -> {
            try (TxnContext context = corfuStore.txn(COMPACTION_NAMESPACE)) {
                context.putRecord(statusTable, keyMsg, statusMsg, null);
                context.commit();
            } catch (TransactionAbortedException tae) {
                log.trace("Hit an TAE when updating checkpoint task status, retry num is {}",
                        retryCount.get(), tae);

                if (retryCount.decrementAndGet() < 0) {
                    throw new RetryExhaustedException(buildExceptionMsg(keyMsg, statusMsg), tae);
                } else {
                    throw new RetryNeededException();
                }
            }
            return null;
        }).setOptions(x -> x.setMaxRetryThreshold(STATUS_RETRY_MAX_DURATION)).run();
    }

    private String buildExceptionMsg(StatusKeyMsg keyMsg, StatusMsg statusMsg) {
        return new StringBuilder(EXCEPTION_MESSAGE)
                .append(" after ")
                .append(STATUS_RETRY_NUM)
                .append(" times retry! Task is ")
                .append(keyMsg.toString())
                .append(" and status is ")
                .append(statusMsg.getStatus())
                .toString();
    }
}
