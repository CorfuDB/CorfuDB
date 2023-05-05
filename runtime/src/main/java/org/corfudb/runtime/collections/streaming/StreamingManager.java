package org.corfudb.runtime.collections.streaming;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplicationListener;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.corfudb.runtime.LogReplicationUtils.LR_STATUS_STREAM_TAG;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * A streaming subscription manager that allows clients to listen on
 * the transaction updates of interested tables. The updates will be
 * streamlined and clients can get notifications via the registered
 * call backs.
 * <p>
 */
@Slf4j
public class StreamingManager {

    private final CorfuRuntime runtime;

    private final StreamPollingScheduler scheduler;

    private final int defaultBufferSize = 25;

    public StreamingManager(@Nonnull CorfuRuntime runtime) {
        this.runtime = runtime;

        ScheduledExecutorService schedulerThread = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(StreamPollingScheduler.class.getName())
                .build());

        ExecutorService workersPool = Executors.newFixedThreadPool(runtime.getParameters()
                .getStreamingWorkersThreadPoolSize(), new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(StreamPollingScheduler.class.getName() + "-worker-%d")
                .build());

        this.scheduler = new StreamPollingScheduler(runtime, schedulerThread, workersPool,
                runtime.getParameters().getStreamingPollPeriod(),
                runtime.getParameters().getStreamingSchedulerPollBatchSize(),
                runtime.getParameters().getStreamingSchedulerPollThreshold());
    }

    /**
     * Subscribe to transaction updates.
     *
     * @param streamListener   client listener for callback
     * @param namespace        namespace of interested tables
     * @param streamTag        only updates of tables with the stream tag will be polled
     * @param tablesOfInterest only updates from these tables will be returned
     * @param lastAddress      last processed address, new notifications start from lastAddress + 1
     */
    public void subscribe(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                                @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                                long lastAddress) {
        subscribe(streamListener, namespace, streamTag, tablesOfInterest, lastAddress, defaultBufferSize);
    }

    /**
     * Subscribe to transaction updates.
     *
     * @param streamListener   client listener for callback
     * @param namespace        namespace of interested tables
     * @param streamTag        only updates of tables with the stream tag will be polled
     * @param tablesOfInterest only updates from these tables will be returned
     * @param lastAddress      last processed address, new notifications start from lastAddress + 1
     * @param bufferSize       maximum size of buffered transaction entries
     */
    public void subscribe(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                          @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                          long lastAddress, int bufferSize) {
        // TODO(Maithem): LR can fail silently if this subscribe fails
        validateSyncAddress(namespace, streamTag, lastAddress);
        this.scheduler.addTask(streamListener, namespace, streamTag, tablesOfInterest, lastAddress, bufferSize);
    }

    /**
     * This subscription is exclusive to Log Replication(LR).  It is used for subscribing to transaction updates across
     * LR's metadata table(ReplicationStatus) in the CorfuSystem namespace and application-specific tables containing
     * 'streamTag' within the application namespace.
     *
     * @param streamListener   client listener for callback
     * @param namespace       namespace of application tables on which updates should be received
     * @param streamTag       only updates of application tables with the stream tag will be polled
     * @param tablesOfInterest only updates from these tables will be returned
     * @param lastAddress      last processed address, new notifications start from lastAddress + 1
     */
    public void subscribeLogReplicationListener(@Nonnull LogReplicationListener streamListener, @Nonnull String namespace,
                                                @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                                                long lastAddress, int bufferSize) {
        Map<String, List<String>> nsToTableName = new HashMap<>();
        nsToTableName.put(namespace, tablesOfInterest);
        nsToTableName.put(CORFU_SYSTEM_NAMESPACE, Arrays.asList(REPLICATION_STATUS_TABLE_NAME));

        Map<String, String> nsToStreamTags = new HashMap<>();
        nsToStreamTags.put(namespace, streamTag);
        nsToStreamTags.put(CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);
        this.scheduler.addLRTask(streamListener, nsToStreamTags, nsToTableName, lastAddress,
                bufferSize == 0 ? defaultBufferSize : bufferSize);
    }


    // TODO(Maithem): this is obsolete, delta stream already detects this case, but can't be
    // removed until some tests that depend on this internal behavior are fixed
    private void validateSyncAddress(String namespace, String streamTag, long lastAddress) {
        long syncAddress = lastAddress + 1;

        UUID txnStreamId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);
        StreamAddressSpace streamAddressSpace = runtime.getSequencerView()
                .getStreamAddressSpace(new StreamAddressRange(txnStreamId, Address.MAX, syncAddress));

        if (streamAddressSpace.getTrimMark() == Address.NON_ADDRESS) {
            // Fix this
            return;
        }
        if (syncAddress <= streamAddressSpace.getTrimMark()) {
            TrimmedException te = new TrimmedException(String.format("Subscription Stream[%s$tag:%s][%s] :: sync start address falls " +
                            "behind trim mark. This will incur in data loss for data in the space [%s, %s] (inclusive)",
                    namespace, streamTag, Utils.toReadableId(txnStreamId), syncAddress, streamAddressSpace.getTrimMark()));
            throw new StreamingException(te, StreamingException.ExceptionCause.TRIMMED_EXCEPTION);
        }
    }

    /**
     * Unsubscribe a prior subscription.
     *
     * @param streamListener client listener to unsubscribe
     */
    public void unsubscribe(@Nonnull StreamListener streamListener) {
        // undefined behavior carried from old implementation
        // log warn message if it didnt exist?
        this.scheduler.removeTask(streamListener);
    }

    /**
     * Shutdown the streaming manager and clean up resources.
     */
    public synchronized void shutdown() {
        this.scheduler.shutdown();
    }
}
