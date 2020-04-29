package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.StreamSubscriptionException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.IStreamView;

/**
 * A simple thread based subscription engine where each subscriber or listener gets
 * a thread which will listen on the tables of interest to it and moves at a rate
 * which is the same as the consumption.
 *
 * Created by hisundar on 04/28/2020.
 */
@Slf4j
public class StreamManager {

    private static final int DEFAULT_NUM_SUBSCRIBERS = 6; // Max number of threads.
    private static final int MAX_SUBSCRIBERS = 32; // Arbitrary sanity check value.
    private static final int DEFAULT_SLEEP_MILLIS = 50; // Default time to sleep
    public static final int DEFAULT_LONG_RUNNING_TIME_SECS = 1; // When to log long running onNext()
    /**
     * The actual executor thread pool where the subscribers will run.
     */
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * Map of StreamingSubscriptionThreads.
     * <p>
     * This is the map of all streaming subscriptions.
     */
    @Getter
    private final Map<StreamListener, StreamSubscriber> subscriptions;

    /**
     * Corfu Runtime.
     */
    @Getter
    private final CorfuRuntime runtime;

    @AllArgsConstructor
    @Getter
    private class StreamSubscriber {
        private final SubscriberTask subscriberTask;
        private final ScheduledFuture<?> scheduledFuture;
    }

    public StreamManager(@Nonnull CorfuRuntime runtime, int maxSubscribers) {
        this.runtime = runtime;
        this.subscriptions = new HashMap<>();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(maxSubscribers);
    }

    public StreamManager(@Nonnull CorfuRuntime runtime) {
        this(runtime, DEFAULT_NUM_SUBSCRIBERS);
    }

    public void shutdown() {
        log.info("Shutting down StreamManager executor pool");
        scheduledExecutorService.shutdown();
    }

    /**
     * Subscribe to updates.
     *
     * @param streamListener   Client listener.
     * @param namespace        Namespace of interest.
     * @param tablesOfInterest Only updates from these tables will be returned.
     * @param startAddress     Address to start the notifications from.
     */
    public synchronized <K extends Message, V extends Message, M extends Message>
    void subscribe(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                   @Nonnull List<TableSchema<K, V, M>> tablesOfInterest, long startAddress) {
        if (subscriptions.containsKey(streamListener)) {
            // Multiple subscribers subscribing to same namespace and table is allowed.
            // Which is why we use the caller's hashcode() and equals() are used to validate re-subscription.
            // If the caller does not want to start another subscriber on the same namespace & table,
            // then the hashcode should be constructed from listener name.
            // If caller wants to allow 2 subscriptions on the same namespace+table,
            // then hashcode and equals can just reflect the subscriber's name or other distinguishing fields.
            // Basically we are saying, "hey you decide if this is a re-subscription or a new subscription"
            throw new StreamSubscriptionException(
                    "StreamManager:subscriber already registered "
                            + streamListener + ". Maybe call unregister first?");
        }
        if (subscriptions.size() >= MAX_SUBSCRIBERS) {
            log.error("StreamManager::subscribe {} has too many {} subscribers",
                    streamListener, MAX_SUBSCRIBERS);
            throw new StreamSubscriptionException(
                    "StreamManager: too many (" + MAX_SUBSCRIBERS + ") subscriptions");
        }
        log.info("StreamManager::subscribe {}, startAddress {}, namespace {}, tables {}",
                streamListener, startAddress, namespace, tablesOfInterest.toString());
        SubscriberTask task = new SubscriberTask(this,
                streamListener, namespace, tablesOfInterest, startAddress);
        final ScheduledFuture<?> scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(
                task, 0, DEFAULT_SLEEP_MILLIS, TimeUnit.MILLISECONDS);
        subscriptions.put(streamListener, new StreamSubscriber(task, scheduledFuture));
    }

    /**
     * Unsubscribe a prior subscription.
     *
     * @param streamListener Client listener.
     */
    public void unsubscribe(@Nonnull StreamListener streamListener) {
        StreamSubscriber streamSubscriber = unsubscribeInternal(streamListener);
        if (streamSubscriber != null) {
            streamSubscriber.getScheduledFuture().cancel(false);
        }
    }

    /**
     * Internal method that can be safely invoked from the same thread executing the task.
     * @param streamListener client's listener object.
     * @return - returns the stream subscriber context for lifecycle management.
     */
    private synchronized StreamSubscriber unsubscribeInternal(@Nonnull StreamListener streamListener) {
        StreamSubscriber subscriber = subscriptions.remove(streamListener);
        if (subscriber == null) {
            log.warn("StreamManager::unsubscribe has no context for {}", streamListener.toString());
            return null;
        }
        // Since we are not force interrupting the thread, set a flag for it to shutdown
        subscriber.getSubscriberTask().isShutdown.set(true);
        log.info("Unsubscribed StreamListener {}", streamListener.toString());
        runtime.getStreamsView().removeStream(subscriber.getSubscriberTask().txnStream);
        return subscriber;
    }

    class SubscriberTask implements Runnable {
        /**
         * The callback context
         */
        private final StreamListener listener;

        /**
         * Parent class to manage errors.
         */
        @Getter
        private final StreamManager streamManager;

        /**
         * Namespace of interest.
         */
        @Getter
        private final String namespace;

        @Setter
        private final AtomicBoolean isShutdown = new AtomicBoolean(false);

        /**
         * Tables of interest.
         *
         * Map of (Stream Id - TableSchema)
         */
        @Getter
        private final Map<UUID,
                TableSchema<? extends Message, ? extends Message, ? extends Message>> tablesOfInterest;

        /**
         * A reference to the underlying stream
         */
        @Getter
        private final IStreamView txnStream;
        public <K extends Message, V extends Message, M extends Message>
        SubscriberTask(@Nonnull StreamManager streamManager,
                       @Nonnull StreamListener listener,
                       @Nonnull String namespace,
                       @Nonnull List<TableSchema<K, V, M>> tableSchemas,
                       long startAddress) {
            this.streamManager = streamManager;
            this.listener = listener;
            this.namespace = namespace;
            this.tablesOfInterest = new HashMap<>();
            tableSchemas.forEach(tableSchema -> tablesOfInterest.put(
                    CorfuRuntime.getStreamID(
                            TableRegistry.getFullyQualifiedTableName(namespace,
                                    tableSchema.getTableName())
                    ), tableSchema
            ));

            StreamOptions options = StreamOptions.builder()
                    .cacheEntries(false)
                    .build();

            this.txnStream = runtime.getStreamsView()
                    .get(ObjectsView.TRANSACTION_STREAM_ID, options);
            this.txnStream.seek(startAddress + 1);
        }

        public void run() {
            while (!isShutdown.get()) {
                try {
                    Thread.currentThread().setName(namespace + listener.toString());
                    ILogData logData = txnStream.nextUpTo(Address.MAX);
                    if (logData == null) {
                        break; // Stream is all caught up, sleep for a bit.
                    }
                    MultiObjectSMREntry multiObjSMREntry = (MultiObjectSMREntry) logData.getPayload(runtime);
                    long epoch = logData.getEpoch();
                    Map<TableSchema, List<CorfuStreamEntry>> entries = new HashMap<>();
                    // first only filter by the stream IDs of interest that are present in this logData
                    logData.getStreams().stream().filter(tablesOfInterest::containsKey)
                            .forEach(streamId -> entries.put(tablesOfInterest.get(streamId),
                                    // Only extract the list of updates per stream as a list
                                    multiObjSMREntry.getSMRUpdates(streamId).stream().map(smrEntry ->
                                            CorfuStreamEntry.fromSMREntry(smrEntry,
                                                    epoch,
                                                    tablesOfInterest.get(streamId).getKeyClass(),
                                                    tablesOfInterest.get(streamId).getPayloadClass(),
                                                    tablesOfInterest.get(streamId).getMetadataClass())
                                    ).collect(Collectors.toList())));

                    if (!entries.isEmpty()) {
                        CorfuStreamEntries callbackResult = new CorfuStreamEntries(entries);
                        log.trace("{}::onNext with {} updates", listener.toString(), entries.size());
                        long onNextStart = System.nanoTime();
                        listener.onNext(callbackResult);
                        long onNextEnd = System.nanoTime();
                        if (TimeUnit.NANOSECONDS.toSeconds(onNextEnd - onNextStart) >=
                                StreamManager.DEFAULT_LONG_RUNNING_TIME_SECS) {
                            log.info("{}::onNext took {}s", listener.toString(),
                                    TimeUnit.NANOSECONDS.toSeconds(onNextEnd - onNextStart));
                        }
                    }
                } catch (Throwable throwable) {
                    log.warn("{}::onError: {}", listener.toString(), throwable.toString());
                    // Since we want to allow a caller to re-subscribe onError() we must not cancel
                    // the thread right away. Instead remove the subscription context to allow resubscribe.
                    StreamSubscriber self = streamManager.unsubscribeInternal(listener);
                    listener.onError(throwable);
                    // Finally just cancel this Runnable task.
                    self.getScheduledFuture().cancel(false);
                    break;
                }
            } // repeat until shutdown
        }
    }
}
