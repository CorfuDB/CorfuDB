package org.corfudb.infrastructure.log;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The stream log compactor reclaims disk spaces by leveraging
 * the garbage information identified by the runtime.
 * <p>
 * Created by WenbinZhu on 5/22/19.
 */
public class StreamLogCompactor {

    private final StreamLogParams logParams;

    private final CompactionPolicy compactionPolicy;

    private final SegmentManager segmentManager;

    private final ScheduledExecutorService compactionScheduler;

    private final ExecutorService compactionWorker;

    public StreamLogCompactor(StreamLogParams logParams) {
        this.logParams = logParams;
        this.compactionPolicy = CompactionPolicy.getPolicy(logParams);
        this.segmentManager = new SegmentManager(logParams);

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("LogUnit-Compactor-%d")
                .build());
        compactionWorker = Executors.newFixedThreadPool(logParams.compactorWorker);

        compactionScheduler.scheduleAtFixedRate(this::runCompactor, logParams.compactorInitialDelay,
                logParams.compactorPeriod, logParams.compactorTimeUnit);
    }

    private void runCompactor() {
        List<StreamLogSegment> segments =
                compactionPolicy.getSegmentsToCompact(segmentManager.getCompactibleSegments());

    }

    public void shutdown() {

    }
}
