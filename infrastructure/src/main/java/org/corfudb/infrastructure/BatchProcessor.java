package org.corfudb.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.TextFormat;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.BatchWriterOperation.Type;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.CorfuProtocolLogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.corfudb.protocols.CorfuProtocolLogData.getLogData;
import static org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;

/**
 * This class manages access for operations that need ordering while executing against
 * the backing storage.
 */
@Slf4j
public class BatchProcessor implements AutoCloseable {

    private final int BATCH_SIZE;
    private final boolean sync;
    private final StreamLog streamLog;
    private final BlockingQueue<BatchWriterOperation> operationsQueue;
    private final ExecutorService processorService;

    private final Optional<Timer> writeRecordTimer;
    private final Optional<Timer> writeRecordsTimer;
    private final Optional<DistributionSummary> queueSizeDist;

    /**
     * The sealEpoch is the epoch up to which all operations have been sealed. Any
     * BatchWriterOperation arriving after the sealEpoch with an epoch less than the sealEpoch
     * is completed exceptionally with a WrongEpochException.
     * This is persisted in the ServerContext by the LogUnitServer to withstand restarts.
     */
    private long sealEpoch;

    /**
     * Returns a new BatchProcessor for a stream log.
     *
     * @param streamLog stream log for writes (can be in memory or file)
     * @param sealEpoch All operations stamped with epoch less than the epochWaterMark are discarded.
     * @param sync      If true, the batch writer will sync writes to secondary storage
     */
    public BatchProcessor(StreamLog streamLog, long sealEpoch, boolean sync) {
        this.sealEpoch = sealEpoch;
        this.sync = sync;
        this.streamLog = streamLog;

        BATCH_SIZE = 50;
        operationsQueue = new LinkedBlockingQueue<>();
        writeRecordTimer = MeterRegistryProvider.getInstance().map(registry ->
                Timer.builder("logunit.write.timer")
                        .publishPercentiles(0.50, 0.95, 0.99)
                        .publishPercentileHistogram()
                        .tags("type", "single").register(registry));
        writeRecordsTimer = MeterRegistryProvider.getInstance().map(registry ->
                Timer.builder("logunit.write.timer")
                        .publishPercentiles(0.50, 0.95, 0.99)
                        .publishPercentileHistogram()
                        .tags("type", "multiple").register(registry));
        queueSizeDist = MeterRegistryProvider.getInstance().map(registry ->
                DistributionSummary
                        .builder("logunit.queue.size")
                        .publishPercentiles(0.50, 0.95, 0.99)
                        .publishPercentileHistogram()
                        .baseUnit("op")
                        .register(registry));

        processorService = Executors
                .newSingleThreadExecutor(new ThreadFactoryBuilder()
                        .setDaemon(false)
                        .setNameFormat("LogUnit-BatchProcessor-%d")
                        .build());

        processorService.submit(this::process);
    }

    private void recordRunnable(Runnable runnable, Optional<Timer> timer) {
        if (timer.isPresent()) {
            timer.get().record(runnable);
        } else {
            runnable.run();
        }
    }

    /**
     * Add a task to the processor.
     *
     * @param type The request type
     * @param req  The request message
     * @return     returns a future result for the request, if it expects one
     */
    public <T> CompletableFuture<T> addTask(@Nonnull Type type, @Nonnull RequestMsg req) {
        BatchWriterOperation<T> op = new BatchWriterOperation<>(type, req);
        operationsQueue.add(op);
        return op.getFutureResult();
    }

    private void process() {
        if (!sync) {
            log.warn("batchWriteProcessor: writes configured to not sync with secondary storage");
        }

        try {
            BatchWriterOperation lastOp = null;
            List<BatchWriterOperation<?>> res = new ArrayList<>();

            while (true) {
                BatchWriterOperation currentOp;
                queueSizeDist.ifPresent(dist -> dist.record(operationsQueue.size()));

                if (lastOp == null) {
                    currentOp = operationsQueue.take();
                } else {
                    currentOp = operationsQueue.poll();

                    if (currentOp == null || res.size() == BATCH_SIZE || currentOp == BatchWriterOperation.SHUTDOWN) {
                        streamLog.sync(sync);
                        if (log.isTraceEnabled()) {
                            log.trace("batchWriteProcessor: completed {} operations", res.size());
                        }
                        // At this point we need to complete the requests
                        // that completed successfully (i.e. haven't failed)
                        for (BatchWriterOperation op : res) {
                            if (!op.getFutureResult().isCompletedExceptionally()
                                    && !op.getFutureResult().isCancelled()) {
                                op.getFutureResult().complete(op.getResultValue());
                            }
                        }

                        res.clear();
                    }
                }

                if (currentOp == null) {
                    lastOp = null;
                } else if (currentOp == BatchWriterOperation.SHUTDOWN) {
                    log.warn("batchWriteProcessor: shutting down the write processor");
                    streamLog.sync(true);
                    break;
                } else if (streamLog.quotaExceeded() &&
                        (currentOp.getRequest().getHeader().getPriority() != PriorityLevel.HIGH)) {
                    currentOp.getFutureResult().completeExceptionally(
                            new QuotaExceededException("Quota of " + streamLog.quotaLimitInBytes() + " bytes"));

                    log.warn("batchWriteProcessor: quota exceeded, dropping request {}",
                            TextFormat.shortDebugString(currentOp.getRequest()));
                } else if (currentOp.getType() == BatchWriterOperation.Type.SEAL &&
                        (currentOp.getRequest().getPayload().getSealRequest().getEpoch() >= sealEpoch)) {
                    log.info("batchWriteProcessor: updating epoch from {} to {}",
                            sealEpoch, currentOp.getRequest().getPayload().getSealRequest().getEpoch());

                    sealEpoch = currentOp.getRequest().getPayload().getSealRequest().getEpoch();
                    res.add(currentOp);
                    lastOp = currentOp;
                } else if (currentOp.getRequest().getHeader().getEpoch() != sealEpoch) {
                    log.warn("batchWriteProcessor: wrong epoch on {} request, seal epoch is {}, and request epoch is {}",
                            currentOp.getType(), sealEpoch, currentOp.getRequest().getHeader().getEpoch());

                    currentOp.getFutureResult().completeExceptionally(new WrongEpochException(sealEpoch));
                    lastOp = currentOp;
                } else {
                    try {
                        RequestPayloadMsg payload =  currentOp.getRequest().getPayload();
                        switch (currentOp.getType()) {
                            case PREFIX_TRIM:
                                final long addr = payload.getTrimLogRequest().getAddress().getSequence();
                                streamLog.prefixTrim(addr);
                                break;
                            case WRITE:
                                LogData logData = getLogData(payload.getWriteLogRequest().getLogData());
                                Runnable append = () -> streamLog.append(logData.getGlobalAddress(), logData);
                                recordRunnable(append, writeRecordTimer);
                                break;
                            case RANGE_WRITE:
                                List<LogData> range = payload.getRangeWriteLogRequest().getLogDataList()
                                        .stream().map(CorfuProtocolLogData::getLogData).collect(Collectors.toList());
                                Runnable appendMultiple = () -> streamLog.append(range);
                                recordRunnable(appendMultiple, writeRecordsTimer);
                                break;
                            case RESET:
                                streamLog.reset();
                                break;
                            case TAILS_QUERY:
                                final TailsResponse tails;

                                switch (payload.getTailRequest().getReqType()) {
                                    case LOG_TAIL:
                                        tails = new TailsResponse(streamLog.getLogTail());
                                        break;
                                    case ALL_STREAMS_TAIL:
                                        tails = streamLog.getAllTails();
                                        break;
                                    default:
                                        throw new UnsupportedOperationException("Unknown request type "
                                                + payload.getTailRequest().getReqType());
                                }

                                tails.setEpoch(sealEpoch);
                                currentOp.setResultValue(tails);
                                break;
                            case LOG_ADDRESS_SPACE_QUERY:
                                // Retrieve the address space for every stream in the log.
                                StreamsAddressResponse resp = streamLog.getStreamsAddressSpace();
                                resp.setEpoch(sealEpoch);
                                currentOp.setResultValue(resp);
                                break;
                            default:
                                log.warn("batchWriteProcessor: unknown operation {}", currentOp);
                        }
                    } catch (Exception e) {
                        log.error("batchWriteProcessor: stream log error. Batch: [queue size={}]. " +
                                "StreamLog: [trim mark={}].", operationsQueue.size(), streamLog.getTrimMark(), e);

                        currentOp.getFutureResult().completeExceptionally(e);
                    }

                    res.add(currentOp);
                    lastOp = currentOp;
                }
            }
        } catch (Exception e) {
            log.error("Caught exception in the write processor ", e);
        }
    }

    @Override
    public void close() {
        operationsQueue.add(BatchWriterOperation.SHUTDOWN);
        processorService.shutdown();
        try {
            processorService.awaitTermination(ServerContext.SHUTDOWN_TIMER.toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError("BatchProcessor close interrupted.", e);
        }
    }
}
