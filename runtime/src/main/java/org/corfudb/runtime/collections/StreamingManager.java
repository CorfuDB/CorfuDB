package org.corfudb.runtime.collections;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import java.util.HashMap;
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
 * Created by WenbinZhu on 11/5/20.
 */
@Slf4j
public class StreamingManager {

    // Corfu runtime to interact with corfu streams.
    private final CorfuRuntime runtime;

    // A map of all stream listeners and their subscription contexts.
    private final Map<StreamListener, StreamSubscription> subscriptions;

    // Thread pool for executing stream polling tasks.
    private final ScheduledExecutorService pollingExecutor;

    // Thread pool for executing client call back tasks.
    private final ExecutorService notificationExecutor;

    /**
     * Create the stream manager, initialize the tasks pools.
     *
     * @param runtime Corfu runtime to use for streaming
     */
    public StreamingManager(@Nonnull CorfuRuntime runtime) {
        this.runtime = runtime;
        this.subscriptions = new HashMap<>();

        this.pollingExecutor = Executors.newScheduledThreadPool(runtime.getParameters().getStreamingPollingThreadPoolSize(),
                new ThreadFactoryBuilder().setNameFormat("streaming-poller-%d").build());
        this.notificationExecutor = Executors.newFixedThreadPool(runtime.getParameters().getStreamingNotificationThreadPoolSize(),
                new ThreadFactoryBuilder().setNameFormat("streaming-notifier-%d").build());
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
    synchronized void subscribe(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                                @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                                long lastAddress) {
        subscribe(streamListener, namespace, streamTag, tablesOfInterest, lastAddress, runtime.getParameters().getStreamingQueueSize());
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
    synchronized void subscribe(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                                @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                                long lastAddress, int bufferSize) {
        if (bufferSize < 1) {
            throw new IllegalArgumentException("subscribe: Buffer size cannot be less than 1.");
        }

        // Before starting, validate that the address to seek to is not already behind the trim mark,
        // otherwise, throw a TrimmedException (wrapped in Streaming Exception)
        validateSyncAddress(namespace, streamTag, lastAddress);

        if (subscriptions.containsKey(streamListener)) {
            // Multiple subscribers subscribing to same namespace and table is allowed
            // as long as the hashcode() and equals() method of the listeners are different.
            throw new StreamingException(
                    "StreamingManager::subscribe: listener already registered " + streamListener);
        }

        StreamSubscription subscription = new StreamSubscription(
                runtime, streamListener, namespace, streamTag, tablesOfInterest, bufferSize);
        subscriptions.put(streamListener, subscription);

        pollingExecutor.submit(new StreamPollingTask(this, lastAddress, subscription, pollingExecutor,
                runtime.getParameters()));
        notificationExecutor.submit(new StreamNotificationTask(this, subscription, notificationExecutor, runtime.getParameters()));

        log.info("Subscribed stream listener {}, numSubscribers: {}, streamTag: {}, lastAddress: {}, " +
                        "namespace {}, tables {}", streamListener, subscriptions.size(), streamTag, lastAddress,
                namespace, tablesOfInterest);
    }

    private void validateSyncAddress(String namespace, String streamTag, long lastAddress) {
        long syncAddress = lastAddress + 1;

        UUID txnStreamId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);
        StreamAddressSpace streamAddressSpace = runtime.getSequencerView()
                .getStreamAddressSpace(new StreamAddressRange(txnStreamId, Address.MAX, syncAddress));

        if (syncAddress <= streamAddressSpace.getTrimMark()) {
            TrimmedException te = new TrimmedException(String.format("Subscription Stream[%s$tag:%s][%s] :: sync start address falls " +
                    "behind trim mark. This will incur in data loss for data in the space [%s, %s] (inclusive)",
                    namespace, streamTag, Utils.toReadableId(txnStreamId), syncAddress, streamAddressSpace.getTrimMark()));
            throw new StreamingException(te);
        }
    }

    /**
     * Unsubscribe a prior subscription.
     *
     * @param streamListener client listener to unsubscribe
     */
    synchronized void unsubscribe(@Nonnull StreamListener streamListener) {
        unsubscribe(streamListener, true);
    }

    /**
     * Unsubscribe a prior subscription.
     *
     * @param streamListener            client listener to unsubscribe
     * @param warnIfNotSubscribedBefore whether to log warning if listener is not
     *                                  subscribed or already unsubscribed
     */
    synchronized void unsubscribe(@Nonnull StreamListener streamListener,
                                  boolean warnIfNotSubscribedBefore) {
        StreamSubscription subscription = subscriptions.remove(streamListener);
        if (subscription == null) {
            if (warnIfNotSubscribedBefore) {
                log.warn("StreamingManager::unsubscribe: listener {} not subscribed before", streamListener);
            }
            return;
        }
        subscription.stop();
        log.info("Unsubscribed stream listener {}", streamListener);
        runtime.getStreamsView().removeStream(subscription.getTxnStream());
    }

    /**
     * Shutdown the streaming manager and clean up resources.
     */
    public synchronized void shutdown() {
        log.info("Shutting down StreamingManager.");
        notificationExecutor.shutdown();
        pollingExecutor.shutdown();
    }
}
