package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;

/**
 * BatchWriter is a class that will intercept write-through calls to batch and
 * sync writes.
 */
@Slf4j
public class BatchWriter implements AutoCloseable {

    static final int BATCH_SIZE = 50;

    private final StreamLog streamLog;

    private final LoadingCache<Long, ILogData> cache;

    private BlockingQueue<BatchWriterOperation> operationsQueue;
    final ExecutorService writerService = Executors
            .newSingleThreadExecutor(new ThreadFactoryBuilder()
                    .setDaemon(false)
                    .setNameFormat("LogUnit-Write-Processor-%d")
                    .build());

    /**
     * Returns a new BatchWriter for a stream log.
     *
     * @param streamLog stream log for writes (can be in memory or file)
     */
    public BatchWriter(StreamLog streamLog, LoadingCache<Long, ILogData> cache) {
        this.streamLog = streamLog;
        this.cache = cache;
        operationsQueue = new LinkedBlockingQueue<>();
        writerService.submit(this::batchWriteProcessor);
    }

    /**
     * Add an operation to the batch writer queue
     * @param op Batch writer operation
     */
    public void add(BatchWriterOperation op) {
        operationsQueue.add(op);
    }

    private void completeOperation(BatchWriterOperation op) {
        log.trace("Completing operation {}", op);
        if (op.getException() == null) {
            if (op.getLogData() != null) {
                cache.put(op.getAddress(), op.getLogData());
            } else if (op.getEntries() != null) {
                for (LogData ld : op.getEntries()) {
                    cache.put(ld.getGlobalAddress(), ld);
                }
            }

            op.getRouter().sendResponse(op.getCtx(), op.getMsg(), CorfuMsgType.ACK.msg());

        } else if (op.getException() instanceof TrimmedException) {
            op.getRouter().sendResponse(op.getCtx(), op.getMsg(), CorfuMsgType.ERROR_TRIMMED.msg());
        } else if (op.getException() instanceof OverwriteException) {
            op.getRouter().sendResponse(op.getCtx(), op.getMsg(), CorfuMsgType.ERROR_OVERWRITE.msg());
        } else if (op.getException() instanceof DataOutrankedException) {
            op.getRouter().sendResponse(op.getCtx(), op.getMsg(), CorfuMsgType.ERROR_DATA_OUTRANKED.msg());
        } else if (op.getException() instanceof ValueAdoptedException) {
            op.getRouter().sendResponse(op.getCtx(), op.getMsg(), CorfuMsgType.ERROR_VALUE_ADOPTED
                    .payloadMsg(((ValueAdoptedException) op.getException()).getReadResponse()));
        }
    }

    private void batchWriteProcessor() {
        try {
            BatchWriterOperation lastOp = null;
            int processed = 0;
            List<BatchWriterOperation> res = new LinkedList();

            while (true) {
                BatchWriterOperation currOp;

                if (lastOp == null) {
                    currOp = operationsQueue.take();
                } else {
                    currOp = operationsQueue.poll();

                    if (currOp == null || processed == BATCH_SIZE
                            || currOp == BatchWriterOperation.SHUTDOWN) {
                        streamLog.sync(true);
                        log.trace("Sync'd {} writes", processed);

                        for (BatchWriterOperation operation : res) {
                            completeOperation(operation);
                        }
                        res.clear();
                        processed = 0;
                    }
                }

                if (currOp == null) {
                    lastOp = null;
                    continue;
                } else if (currOp == BatchWriterOperation.SHUTDOWN) {
                    log.trace("Shutting down the write processor");
                    streamLog.sync(true);
                    break;
                } else {
                    try {
                        switch (currOp.getType()) {
                            case TRIM:
                                streamLog.trim(currOp.getAddress());
                                res.add(currOp);
                                break;
                            case PREFIX_TRIM:
                                streamLog.prefixTrim(currOp.getAddress());
                                res.add(currOp);
                                break;
                            case WRITE:
                                streamLog.append(currOp.getAddress(), currOp.getLogData());
                                res.add(currOp);
                                break;
                            case RANGE_WRITE:
                                streamLog.append(currOp.getEntries());
                                res.add(currOp);
                                break;
                            default:
                                log.warn("Unknown BatchWriterOperation {}", currOp);
                        }
                    } catch (Exception e) {
                        currOp.setException(e);
                        res.add(currOp);
                    }

                    processed++;
                    lastOp = currOp;
                }
            }
        } catch (Exception e) {
            log.error("Caught exception in the write processor {}", e);
        }
    }

    @Override
    public void close() {
        operationsQueue.add(BatchWriterOperation.SHUTDOWN);
        writerService.shutdown();
    }

}