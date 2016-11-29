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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by m on 11/30/16.
 */
@Slf4j
public class BatchWriter <K, V> implements CacheWriter<K, V> {

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
            int batchSize = 100;
            int processed = 0;

            List<CompletableFuture> ack = new LinkedList();
            List<CompletableFuture> err = new LinkedList();

            while (true) {
                WriteOperation currOp;

                if (lastOp == null) {
                    currOp = writeQueue.take();
                } else {
                    currOp = writeQueue.poll();

                    if (currOp == null || processed == batchSize || currOp == WriteOperation.SHUTDOWN) {
                        streamLog.sync();
                        log.info("Sync'd {} writes", processed);

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
                    log.info("Shutting down the write processor");
                    break;
                }
                if (currOp == null) {
                    lastOp = null;
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

    void close() {
        writeQueue.add(WriteOperation.SHUTDOWN);
        writerService.shutdown();
    }

}
