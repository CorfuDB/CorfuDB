package org.corfudb.infrastructure.log;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.BatchProcessor;
import org.corfudb.infrastructure.BatchWriterOperation;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.util.LambdaUtils;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LogMetadataRecorder {

    private final BatchProcessor batchProcessor;

    private final ScheduledExecutorService recorder;

    private static final Duration LOG_METADATA_RECORDER_INTERVAL = Duration.ofSeconds(30);

    public LogMetadataRecorder(BatchProcessor batchProcessor) {
        this.batchProcessor = batchProcessor;
        this.recorder = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("LogMetadataRecorder")
                .build());
        start();
    }

    private void start() {
        recorder.scheduleAtFixedRate(
            () -> LambdaUtils.runSansThrow(this::triggerDump),
            LogMetadataRecorder.LOG_METADATA_RECORDER_INTERVAL.toMillis() / 2,
            LogMetadataRecorder.LOG_METADATA_RECORDER_INTERVAL.toMillis(),
            TimeUnit.MILLISECONDS
        );
    }

    public void shutdown() {
        recorder.shutdownNow();
        log.info("LogMetadataRecorder shutting down.");
    }

    private void triggerDump() {
        batchProcessor.addTask(BatchWriterOperation.Type.DUMP_LOG_METADATA,
            CorfuMessage.RequestMsg.newBuilder()
                .setHeader(CorfuMessage.HeaderMsg.newBuilder()
                        .setEpoch(batchProcessor.getSealEpoch())
                        .build()).build());
    }

}
