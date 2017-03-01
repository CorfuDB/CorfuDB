package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.LogAddress;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.OverwriteException;

import javax.annotation.Nonnull;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * BatchWriter is a class that will intercept write-through calls to batch and
 * sync writes.
 */
@Slf4j
public class BatchWriter <K, V> implements CacheWriter<K, V>, AutoCloseable {

    static final int BATCH_SIZE = 50;
    private StreamLog streamLog;
    private BlockingQueue<WriteOperation> writeQueue;
    final ExecutorService writerService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setDaemon(false)
            .setNameFormat("LogUnit-Write-Processor-%d")
            .build());

    public BatchWriter(StreamLog streamLog) {
        this.streamLog = streamLog;
        writeQueue = new LinkedBlockingQueue<>();
        writerService.submit(this::batchWriteProcessor);
    }

    @Override
    public void write(@Nonnull K key, @Nonnull V value) {
        try {
            submitWrite((LogAddress) key, (LogData) value).get();
        } catch (Exception e) {
            log.trace("Write Exception {}", e);

            if(e.getCause() instanceof OverwriteException) {
                throw new OverwriteException();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void delete(K key, V value, RemovalCause removalCause) {}

    private CompletableFuture<Void> submitWrite(LogAddress address, LogData logData) {
        CompletableFuture<Void> cf = new CompletableFuture();
        writeQueue.add(new WriteOperation(address, logData,cf));
        return cf;
    }

    private void batchWriteProcessor() {
        try {
            WriteOperation lastOp = null;

            int processed = 0;

            List<CompletableFuture> ack = new LinkedList();
            List<CompletableFuture> err = new LinkedList();

            while (true) {
                WriteOperation currOp;

                if (lastOp == null) {
                    currOp = writeQueue.take();
                } else {
                    currOp = writeQueue.poll();

                    if (currOp == null || processed == BATCH_SIZE || currOp.getFlush() || currOp == WriteOperation.SHUTDOWN) {
                        streamLog.sync();
                        log.trace("Sync'd {} writes", processed);

                        for(CompletableFuture cf : ack) {
                            cf.complete(null);
                        }

                        for(CompletableFuture cf : err) {
                            cf.completeExceptionally(new OverwriteException());
                        }

                        ack.clear();
                        err.clear();
                        processed = 0;
                    }
                }

                if (currOp == WriteOperation.SHUTDOWN) {
                    log.trace("Shutting down the write processor");
                    break;
                }
                if (currOp == null) {
                    lastOp = null;
                    continue;
                }

                if (currOp.getFlush()) {
                    log.trace("Flushing all pending writes");
                    if (currOp.getLogAddress() == null && currOp.getLogData() == null) {
                        currOp.getFuture().complete(null);
                    } else {
                        try {
                            streamLog.append(currOp.getLogAddress(), currOp.getLogData());
                            currOp.getFuture().complete(null);
                        } catch (OverwriteException e) {
                            currOp.getFuture().completeExceptionally(new OverwriteException());
                        }
                    }
                    lastOp = currOp;
                    continue;
                }

                try {
                    streamLog.append(currOp.getLogAddress(), currOp.getLogData());
                    ack.add(currOp.getFuture());
                } catch (OverwriteException e) {
                    err.add(currOp.getFuture());
                }

                processed++;
                lastOp = currOp;
            }
        } catch (Exception e) {
            log.error("Caught exception in the write processor {}", e);
        }
    }

    /**
     * Issues a flush message to the writeQueue and waits for it to be processed.
     */
    void flushPendingWrites() {
        CompletableFuture cf = new CompletableFuture();
        WriteOperation flushOperation = new WriteOperation(null, null, cf, true);
        writeQueue.add(flushOperation);
        // Wait for flush to complete.
        // Periodically check if a shutdown trigger is invoked to avoid deadlock.
        while (true) {
            try {
                cf.get(1000, TimeUnit.MILLISECONDS);
                break;
            } catch (ExecutionException e) {
                log.error("Exception while flushing queue: {}", e.getMessage());
                break;
            } catch (TimeoutException | InterruptedException e) {
                if (writerService.isShutdown()) {
                    break;
                }
            }
        }
    }

    @Override
    public void close() {
        writeQueue.add(WriteOperation.SHUTDOWN);
        writerService.shutdown();
    }

}