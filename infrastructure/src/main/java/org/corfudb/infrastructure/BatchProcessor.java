package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.BatchWriterOperation.Type;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.protocols.wireprotocol.RangeWriteMsg;
import org.corfudb.protocols.wireprotocol.TailsRequest;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.TrimRequest;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

/**
 * This class manages access for operations that need ordering while executing against
 * the backing storage.
 */
@Slf4j
public class BatchProcessor implements AutoCloseable {

    final private int BATCH_SIZE = 50;

    final private boolean sync;

    final private StreamLog streamLog;

    final private BlockingQueue<BatchWriterOperation> operationsQueue;

    private ExecutorService processorService = Executors
            .newSingleThreadExecutor(new ThreadFactoryBuilder()
                    .setDaemon(false)
                    .setNameFormat("LogUnit-BatchProcessor-%d")
                    .build());

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
     * @param streamLog      stream log for writes (can be in memory or file)
     * @param sealEpoch All operations stamped with epoch less than the epochWaterMark are
     *                       discarded.
     * @param streamLog the backing log (can be in memory or file)
     * @param sync    If true, the batch writer will sync writes to secondary storage
     */
    public BatchProcessor(StreamLog streamLog, long sealEpoch, boolean sync) {
        this.sealEpoch = sealEpoch;
        this.sync = sync;
        this.streamLog = streamLog;
        operationsQueue = new LinkedBlockingQueue<>();
        processorService.submit(this::processor);
    }

    /**
     * Add a task to the processor.
     * @param type The request type
     * @param msg The request message
     * @return returns a future result for the request, if it expects one
     */
    public <T> CompletableFuture <T> addTask(@Nonnull Type type, @Nonnull CorfuPayloadMsg msg) {
        BatchWriterOperation<T> operation = new BatchWriterOperation<>(type, msg);
        operationsQueue.add(operation);
        return operation.getFutureResult();
    }

    private void processor() {

        if (!sync) {
            log.warn("batchWriteProcessor: writes configured to not sync with secondary storage");
        }

        try {
            BatchWriterOperation lastOp = null;
            int processed = 0;
            List<BatchWriterOperation> res = new LinkedList<>();

            while (true) {
                BatchWriterOperation currOp;

                if (lastOp == null) {
                    currOp = operationsQueue.take();
                } else {
                    currOp = operationsQueue.poll();

                    if (currOp == null || processed == BATCH_SIZE
                            || currOp == BatchWriterOperation.SHUTDOWN) {
                        streamLog.sync(sync);
                        log.trace("Completed {} operations", processed);

                        for (BatchWriterOperation operation : res) {
                            if (!operation.getFutureResult().isCompletedExceptionally()
                            && !operation.getFutureResult().isCancelled()) {
                                // At this point we need to complete the requests
                                // that completed successfully (i.e. haven't failed)
                                operation.getFutureResult().complete(operation.getResultValue());
                            }
                        }
                        res.clear();
                        processed = 0;
                    }
                }

                if (currOp == null) {
                    lastOp = null;
                } else if (currOp == BatchWriterOperation.SHUTDOWN) {
                    log.warn("Shutting down the write processor");
                    streamLog.sync(true);
                    break;
                } else if (streamLog.quotaExceeded() && currOp.getMsg().getPriorityLevel() != PriorityLevel.HIGH) {
                    currOp.getFutureResult().completeExceptionally(
                            new QuotaExceededException("Quota of "
                                    + streamLog.quotaLimitInBytes() + " bytes"));
                    log.warn("batchprocessor: quota exceeded, dropping msg {}", currOp.getMsg());
                } else if (currOp.getType() == Type.SEAL && currOp.getMsg().getEpoch() >= sealEpoch) {
                    log.info("batchWriteProcessor: updating from {} to {}", sealEpoch, currOp.getMsg().getEpoch());
                    sealEpoch = currOp.getMsg().getEpoch();
                    res.add(currOp);
                    processed++;
                    lastOp = currOp;
                } else if (currOp.getMsg().getEpoch() != sealEpoch) {
                    log.warn("batchWriteProcessor: wrong epoch on {} msg, seal epoch is {}, and msg epoch is {}",
                            currOp.getType(), sealEpoch, currOp.getMsg().getEpoch());
                    currOp.getFutureResult().completeExceptionally(new WrongEpochException(sealEpoch));
                    res.add(currOp);
                    processed++;
                    lastOp = currOp;
                } else {
                    try {
                        switch (currOp.getType()) {
                            case PREFIX_TRIM:
                                TrimRequest prefixTrim = (TrimRequest) currOp.getMsg().getPayload();
                                streamLog.prefixTrim(prefixTrim.getAddress().getSequence());
                                break;
                            case WRITE:
                                WriteRequest write = (WriteRequest) currOp.getMsg().getPayload();
                                streamLog.append(write.getGlobalAddress(), (LogData) write.getData());
                                break;
                            case RANGE_WRITE:
                                RangeWriteMsg writeRange = (RangeWriteMsg) currOp.getMsg().getPayload();
                                streamLog.append(writeRange.getEntries());
                                break;
                            case SUFFIX_TRIM:
                                streamLog.suffixTrim();
                                break;
                            case RESET:
                                streamLog.reset();
                                break;
                            case TAILS_QUERY:
                                TailsRequest tailsRequest = (TailsRequest)currOp.getMsg().getPayload();
                                TailsResponse tails;

                                switch (tailsRequest.getReqType()) {
                                    case TailsRequest.LOG_TAIL:
                                        tails = new TailsResponse(streamLog.getLogTail());
                                        break;

                                    case TailsRequest.STREAMS_TAILS:
                                        tails = streamLog.getTails(tailsRequest.getStreams());
                                        break;

                                    default:
                                        tails = streamLog.getAllTails();
                                        break;
                                }

                                currOp.setResultValue(tails);
                                break;
                            case LOG_ADDRESS_SPACE_QUERY:
                                // Retrieve the address space for every stream in the log.
                                currOp.setResultValue(streamLog.getStreamsAddressSpace());
                                break;
                            default:
                                log.warn("Unknown BatchWriterOperation {}", currOp);
                        }
                    } catch (Exception e) {
                        log.error("Stream log error. Batch [queue size={}]. StreamLog: [trim mark: {}].",
                                operationsQueue.size(), streamLog.getTrimMark(), e);
                        currOp.getFutureResult().completeExceptionally(e);
                    }
                    res.add(currOp);

                    processed++;
                    lastOp = currOp;
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