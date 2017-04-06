package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.plexus.util.ExceptionUtils;
import org.corfudb.infrastructure.log.LogAddress;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;

import javax.annotation.Nonnull;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * BatchWriter is a class that will intercept write-through calls to batch and
 * sync writes.
 */
@Slf4j
public class BatchWriter<K, V> implements CacheWriter<K, V>, AutoCloseable {

    static final int BATCH_SIZE = 50;
    private StreamLog streamLog;
    private BlockingQueue<BatchWriterOperation> operationsQueue;
    final ExecutorService writerService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setDaemon(false)
            .setNameFormat("LogUnit-Write-Processor-%d")
            .build());

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
                    (LogAddress) key, (LogData) value, cf));
            cf.get();
        } catch (Exception e) {
            log.trace("Write Exception {}", e);
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException)e.getCause();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public void trim(@Nonnull LogAddress logAddress) {
        try {
            CompletableFuture<Void> cf = new CompletableFuture();
            operationsQueue.add(new BatchWriterOperation(BatchWriterOperation.Type.TRIM, logAddress, null, cf));
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

                    if (currOp == null || processed == BATCH_SIZE || currOp == BatchWriterOperation.SHUTDOWN) {
                        streamLog.sync(true);
                        log.trace("Sync'd {} writes", processed);

                        for (BatchWriterOperation operation : res) {
                            handleOperationResults(operation);
                        }
                        res.clear();
                        processed = 0;
                    }
                }

                if (currOp == BatchWriterOperation.SHUTDOWN) {
                    log.trace("Shutting down the write processor");
                    break;
                }
                if (currOp == null) {
                    lastOp = null;
                    continue;
                }

                if (currOp.getType() == BatchWriterOperation.Type.TRIM) {
                    streamLog.trim(currOp.getLogAddress());
                    currOp.setException(null);
                    res.add(currOp);
                } else if (currOp.getType() == BatchWriterOperation.Type.WRITE) {
                    try {
                        streamLog.append(currOp.getLogAddress(), currOp.getLogData());
                        currOp.setException(null);
                        res.add(currOp);
                    } catch (OverwriteException | DataOutrankedException e) {
                        currOp.setException(e);
                        res.add(currOp);
                    } catch (Exception e) {
                        currOp.setException(e);
                        res.add(currOp);
                    }
                } else {
                    log.warn("Unknown BatchWriterOperation {}", currOp);
                }

                processed++;
                lastOp = currOp;
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