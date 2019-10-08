package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.LambdaUtils;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This service periodically trims compacted addresses at address space view
 * of sequencer view by replacing the address space view with compacted address
 * space fetched from LogUnit servers.
 * <p>
 * Created by Xin on 10/08/19.
 */
@Slf4j
public class SequencerTrimService implements ManagementService {

    private final ServerContext serverContext;

    @Getter
    private final ScheduledExecutorService sequencerTrimScheduler;
    private final ScheduledExecutorService sequencerBatchTrimScheduler;
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;

    // TODO(xin): Do more experiments about what should be an appropriate number
    private static final int BATCH_SIZE = 5;

    public SequencerTrimService(@NonNull ServerContext serverContext,
                                @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;
        sequencerTrimScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "SequencerTrimService")
                        .build());

        sequencerBatchTrimScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "SequencerBatchTrim")
                        .build());
    }

    private CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }

    @VisibleForTesting
    public void runSequencerTrim(Duration interval) {
        log.trace("Trim sequencer address space at round");
        CorfuRuntime rt = getCorfuRuntime();

        // update layout
        getCorfuRuntime()
                .invalidateLayout()
                .thenApply(serverContext::saveManagementLayout)
                .join();

        // Leverages layout as the leader election.
        // Only primary sequencer runs this service to prevent duplicated sequencer address trims.
        if (!rt.getLayoutView().getLayout().getPrimarySequencer()
                .equals(serverContext.getLocalEndpoint())) {
            return;
        }

        // Gets all streamIds from the primary sequencer server
        List<UUID> streamIds = rt.getSequencerView().getStreamsId();

        // Handles sequencer stream address view in batches.
        List<List<UUID>> batches = Lists.partition(streamIds, BATCH_SIZE);
        if (batches.isEmpty()) {
            return;
        }

        int batchNum = batches.size();
        long batchIntervalInMillis = interval.toMillis() / batchNum;

        // Schedules batch sequencer trim tasks evenly within one period
        for (int i = 0; i < batchNum; ++i) {
            List<UUID> batch = batches.get(i);
            sequencerBatchTrimScheduler
                    .schedule(() -> getCorfuRuntime().getSequencerView().addressSpaceTrimInBatch(batch),
                            i * batchIntervalInMillis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Starts the long running service.
     *
     * @param interval interval to run the service
     */
    @Override
    public void start(Duration interval) {
        log.info("Sequencer trim service starts.");
        sequencerTrimScheduler.scheduleAtFixedRate(
                () -> LambdaUtils.runSansThrow(() -> runSequencerTrim(interval)),
                interval.toMillis(),
                interval.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Clean up.
     */
    @Override
    public void shutdown() {
        sequencerTrimScheduler.shutdownNow();
        sequencerBatchTrimScheduler.shutdownNow();
        log.info("Sequencer trim service is shut down.");
    }

}
