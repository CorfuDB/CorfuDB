package org.corfudb.runtime.collections;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.StreamingException;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    // Number of thread in polling and notification pool.
    private static final int NUM_THREAD_PER_POOL = 4;

    // Default buffer size for each subscription.
    private static final int DEFAULT_BUFFER_SIZE = 50;

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

        this.pollingExecutor = Executors.newScheduledThreadPool(NUM_THREAD_PER_POOL,
                new ThreadFactoryBuilder().setNameFormat("streaming-poller-%d").build());
        this.notificationExecutor = Executors.newFixedThreadPool(NUM_THREAD_PER_POOL,
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
        subscribe(streamListener, namespace, streamTag, tablesOfInterest, lastAddress, DEFAULT_BUFFER_SIZE);
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

        if (subscriptions.containsKey(streamListener)) {
            // Multiple subscribers subscribing to same namespace and table is allowed
            // as long as the hashcode() and equals() method of the listeners are different.
            throw new StreamingException(
                    "StreamingManager::subscribe: listener already registered " + streamListener);
        }

        StreamSubscriptionMetrics metrics = new StreamSubscriptionMetrics(
                runtime, streamListener, namespace, streamTag);
        StreamSubscription subscription = new StreamSubscription(
                runtime, streamListener, namespace, streamTag, tablesOfInterest, bufferSize, metrics);
        subscriptions.put(streamListener, subscription);

        pollingExecutor.submit(new StreamPollingTask(this, lastAddress, subscription, pollingExecutor));
        notificationExecutor.submit(new StreamNotificationTask(this, subscription, notificationExecutor));

        log.info("Subscribed stream listener {}, numSubscribers: {}, streamTag: {}, lastAddress: {}, " +
                "namespace {}, tables {}", streamListener, subscriptions.size(), streamTag, lastAddress,
                namespace, tablesOfInterest);
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

    @VisibleForTesting
    public static int getNumThreadPerPool() {
        return NUM_THREAD_PER_POOL;
    }
}
