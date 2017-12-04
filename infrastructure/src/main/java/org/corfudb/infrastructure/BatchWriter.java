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

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;

/**
 * BatchWriter is a class that will intercept write-through calls to batch and
 * sync writes.
 */
@Slf4j
public class BatchWriter<K, V> implements CacheWriter<K, V>, AutoCloseable {

    static final int BATCH_SIZE = 50;
    private StreamLog streamLog;
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
    public BatchWriter(StreamLog streamLog) {
        this.streamLog = streamLog;
        operationsQueue = new LinkedBlockingQueue<>();
        writerService.submit(this::batchWriteProcessor);
    }

    @Override
    public void write(@Nonnull K key, @Nonnull V value) {
        try {
            CompletableFuture<Void> cf = new CompletableFuture();
            operationsQueue.add(new BatchWriterOperation(BatchWriterOperation.Type.WRITE,
                    (Long) key, (LogData) value, null, cf));
            cf.get();
        } catch (Exception e) {
            log.trace("Write Exception {}", e);
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public void bulkWrite(List<LogData> entries) {
        try {
            CompletableFuture<Void> cf = new CompletableFuture();
            operationsQueue.add(new BatchWriterOperation(BatchWriterOperation.Type.RANGE_WRITE,
                    null, null, entries, cf));
        } catch (Exception e) {
            log.trace("Write Exception {}", e);
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Trim an address from the log.
     *
     * @param address log address to trim
     */
    public void trim(@Nonnull long address) {
        try {
            CompletableFuture<Void> cf = new CompletableFuture();
            operationsQueue.add(new BatchWriterOperation(BatchWriterOperation.Type.TRIM,
                    address, null, null, cf));
            cf.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Trim addresses from log up to a prefix.
     *
     * @param address prefix address to trim to (inclusive)
     */
    public void prefixTrim(@Nonnull long address) {
        try {
            CompletableFuture<Void> cf = new CompletableFuture();
            operationsQueue.add(new BatchWriterOperation(BatchWriterOperation.Type.PREFIX_TRIM,
                    address, null, null, cf));
            cf.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(K key, V value, RemovalCause removalCause) {
    }

    private void handleOperationResults(BatchWriterOperation operation) {
        if (operation.getException() == null) {
            operation.getFuture().complete(null);
        } else {
            operation.getFuture().completeExceptionally(operation.getException());
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
                            handleOperationResults(operation);
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