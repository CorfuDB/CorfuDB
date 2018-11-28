package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.RemovalCause;
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
import org.corfudb.protocols.wireprotocol.RangeWriteMsg;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TrimRequest;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

/**
 * BatchWriter is a class that will intercept write-through calls to batch and
 * sync writes.
 */
@Slf4j
public class BatchWriter implements AutoCloseable {

    static final int BATCH_SIZE = 50;

    final boolean sync;

    private StreamLog streamLog;

    private BlockingQueue<BatchWriterOperation> operationsQueue;

    final ExecutorService writerService = Executors
            .newSingleThreadExecutor(new ThreadFactoryBuilder()
                    .setDaemon(false)
                    .setNameFormat("LogUnit-Write-Processor-%d")
                    .build());

    /**
     * The sealEpoch is the epoch up to which all operations have been sealed. Any
     * BatchWriterOperation arriving after the sealEpoch with an epoch less than the sealEpoch
     * is completed exceptionally with a WrongEpochException.
     * This is persisted in the ServerContext by the LogUnitServer to withstand restarts.
     */
    private long sealEpoch;

    /**
     * Returns a new BatchWriter for a stream log.
     *
     * @param streamLog      stream log for writes (can be in memory or file)
     * @param sealEpoch All operations stamped with epoch less than the epochWaterMark are
     *                       discarded.
     * @param streamLog stream log for writes (can be in memory or file)
     * @param sync    If true, the batch writer will sync writes to secondary storage
     */
    public BatchWriter(StreamLog streamLog, long sealEpoch, boolean sync) {
        this.sealEpoch = sealEpoch;
        this.sync = sync;
        this.streamLog = streamLog;
        operationsQueue = new LinkedBlockingQueue<>();
        writerService.submit(this::batchWriteProcessor);
    }

    public <T> CompletableFuture <T> addTask(Type type, CorfuPayloadMsg msg) {
        BatchWriterOperation<T> operation = new BatchWriterOperation<>(type, msg);
        operationsQueue.add(operation);
        return operation.getFutureResult();
    }

    private void batchWriteProcessor() {

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
                        log.trace("Sync'd {} writes", processed);

                        for (BatchWriterOperation operation : res) {
                            operation.
                            handleOperationResults(operation);
                        }
                        res.clear();
                        processed = 0;
                    }
                }

                if (currOp == null) {
                    lastOp = null;
                } else if (currOp == BatchWriterOperation.SHUTDOWN) {
                    log.trace("Shutting down the write processor");
                    streamLog.sync(true);
                    break;
                } else if (currOp.getType() == Type.SEAL && currOp.getMsg().getEpoch() >= sealEpoch) {
                    sealEpoch = currOp.getMsg().getEpoch();
                    res.add(currOp);
                    processed++;
                    lastOp = currOp;
                } else if (currOp.getMsg().getEpoch() != sealEpoch) {
                    log.warn("batchWriteProcessor: wrong epoch on {} msg, seal epoch is {}",
                            currOp.getType(), currOp.getMsg().getEpoch());
                    currOp.setException(new WrongEpochException(sealEpoch));
                    res.add(currOp);
                    processed++;
                    lastOp = currOp;
                } else {
                    try {
                        switch (currOp.getType()) {
                            case TRIM:
                                TrimRequest pointTrim = (TrimRequest) currOp.getMsg().getPayload();
                                streamLog.trim(pointTrim.getAddress().getSequence());
                                res.add(currOp);
                                break;
                            case PREFIX_TRIM:
                                TrimRequest prefixTrim = (TrimRequest) currOp.getMsg().getPayload();
                                streamLog.prefixTrim(prefixTrim.getAddress().getSequence());
                                res.add(currOp);
                                break;
                            case WRITE:
                                WriteRequest write = (WriteRequest) currOp.getMsg().getPayload();
                                streamLog.append(write.getGlobalAddress(), (LogData) write.getData());
                                res.add(currOp);
                                break;
                            case RANGE_WRITE:
                                RangeWriteMsg writeRange = (RangeWriteMsg) currOp.getMsg().getPayload();
                                streamLog.append(writeRange.getEntries());
                                res.add(currOp);
                                break;
                            case RESET:
                                streamLog.reset();
                                res.add(currOp);
                                break;
                            case TAILS_QUERY:
                                TailsResponse tails = streamLog.getTails();
                                currOp.getFuture().complete(tails);
                                break;
                            default:
                                log.warn("Unknown BatchWriterOperation {}", currOp);
                        }
                    } catch (Exception e) {
                        currOp.getFutureResult().completeExceptionally(e);
                        res.add(currOp);
                    }

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
        writerService.shutdown();
        try {
            writerService.awaitTermination(ServerContext.SHUTDOWN_TIMER.toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError("BatchWriter close interrupted.", e);
        }
    }

}