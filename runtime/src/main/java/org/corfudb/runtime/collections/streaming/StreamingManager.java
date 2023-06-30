package org.corfudb.runtime.collections.streaming;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplicationListener;
import org.corfudb.runtime.LogReplicationRoutingQueueListener;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

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
     * @param nsToTableName    map of table namespace to list of tables of interest
     * @param nsToStreamTags   map of table namespace to the stream tag polled
     * @param lastAddress      last processed address, new notifications start from lastAddress + 1
     * @param bufferSize       maximum size of buffered transaction entries
     */
    public void subscribeLogReplicationListener(@Nonnull LogReplicationListener streamListener, Map<String, List<String>> nsToTableName,
                                                Map<String, String> nsToStreamTags, long lastAddress, int bufferSize) {
        this.scheduler.addLRTask(streamListener, nsToStreamTags, nsToTableName, lastAddress,
                bufferSize == 0 ? defaultBufferSize : bufferSize);
    }

    public void subscribeLogReplicationRoutingQueueListener(@Nonnull LogReplicationRoutingQueueListener streamListener,
                                                            @Nonnull String namespace,long lastAddress, int bufferSize,
                                                            String routingQueueName) {
        Map<String, List<String>> nsToTableName = new HashMap<>();
        nsToTableName.put(CORFU_SYSTEM_NAMESPACE, Arrays.asList(REPLICATION_STATUS_TABLE_NAME));
        // Queue is already opened. Add Routing Queue.
        nsToTableName.put(namespace, Arrays.asList(routingQueueName));

        // TODO: Form the routing queue tag from the client_name input parameter.
        // TODO: Check if we need to modify the addLRTask for multiple tags per namespace
        Map<String, String> nsToStreamTags = new HashMap<>();
        nsToStreamTags.put(CORFU_SYSTEM_NAMESPACE, LR_STATUS_STREAM_TAG);
        nsToStreamTags.put(namespace, REPLICATED_QUEUE_TAG_PREFIX);
        this.scheduler.addLRTask(streamListener, nsToStreamTags, nsToTableName, lastAddress,
                bufferSize == 0 ? defaultBufferSize : bufferSize);
    }

    public void subscribeLogReplicationLrStatusTableListener(@Nonnull LogReplicationRoutingQueueListener streamListener,
                                                            long lastAddress, int bufferSize) {
        Map<String, List<String>> nsToTableName = new HashMap<>();
        nsToTableName.put(CORFU_SYSTEM_NAMESPACE, Arrays.asList(REPLICATION_STATUS_TABLE_NAME));

        Map<String, String> nsToStreamTags = new HashMap<>();
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
     * Checks if a listener has already been subscribed.
     *
     * @param streamListener client listener to validate subscription
     * @return true if listener has already been subscribed
     */
    public boolean isListenerSubscribed(@Nonnull StreamListener streamListener) {
        return this.scheduler.containsTask(streamListener);
    }

    /**
     * Validates the buffer size of a stream.
     *
     * @param bufferSize bufferSize
     * @return true if stream has a sufficiently large buffer size
     */
    public boolean validateBufferSize(int bufferSize) {
        return this.scheduler.hasEnoughBuffer(bufferSize == 0 ? defaultBufferSize : bufferSize);
    }

    /**
     * Shutdown the streaming manager and clean up resources.
     */
    public synchronized void shutdown() {
        this.scheduler.shutdown();
    }
}
