package org.corfudb.runtime.collections;

import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.IStreamView;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A simple thread based subscription engine where each subscriber or listener gets
 * a thread which will listen on the tables of interest to it and moves at a rate
 * which is the same as the consumption.
 * <p>
 * Created by hisundar on 04/28/2020.
 */
@Slf4j
@Deprecated
public class StreamManager {

    private static final int DEFAULT_NUM_SUBSCRIBERS = 6; // Max number of threads.
    public static final int MAX_SUBSCRIBERS = 32; // Arbitrary sanity check value.
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
     * @param streamListener   client listener
     * @param namespace        namespace of interest
     * @param tablesOfInterest only updates from these tables will be returned
     * @param lastAddress      last processed address, new notifications start from lastAddress + 1
     */
    synchronized <K extends Message, V extends Message, M extends Message>
    void subscribe(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                   @Nonnull List<TableSchema<K, V, M>> tablesOfInterest, long lastAddress) {
        if (subscriptions.containsKey(streamListener)) {
            // Multiple subscribers subscribing to same namespace and table is allowed.
            // Which is why we use the caller's hashcode() and equals() are used to validate re-subscription.
            // If the caller does not want to start another subscriber on the same namespace & table,
            // then the hashcode should be constructed from listener name.
            // If caller wants to allow 2 subscriptions on the same namespace+table,
            // then hashcode and equals can just reflect the subscriber's name or other distinguishing fields.
            // Basically we are saying, "hey you decide if this is a re-subscription or a new subscription"
            throw new StreamingException(
                    "StreamManager:subscriber already registered "
                            + streamListener + ". Maybe call unregister first?");
        }
        if (subscriptions.size() >= MAX_SUBSCRIBERS) {
            log.error("StreamManager::subscribe {} has too many {} subscribers",
                    streamListener, MAX_SUBSCRIBERS);
            throw new IllegalStateException(
                    "StreamManager: too many (" + MAX_SUBSCRIBERS + ") subscriptions");
        }
        tablesOfInterest.forEach(t -> {
            log.info("table name = {} ID {}", t.getTableName(), CorfuRuntime.getStreamID(t.getTableName()));
        });
        log.info("StreamManager::subscribe {}, lastAddress {}, namespace {}, tables {}",
                streamListener, lastAddress, namespace, tablesOfInterest.toString());
        SubscriberTask task = new SubscriberTask(this,
                streamListener, namespace, tablesOfInterest, lastAddress);
        final ScheduledFuture<?> scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(
                task, 0, DEFAULT_SLEEP_MILLIS, TimeUnit.MILLISECONDS);
        subscriptions.put(streamListener, new StreamSubscriber(task, scheduledFuture));
    }

    /**
     * Subscribe to updates.
     *
     * @param streamListener   client listener
     * @param namespace        namespace of interest
     * @param streamTag        only updates of tables with the stream tag will be polled
     * @param tablesOfInterest only updates from these tables will be returned
     * @param lastAddress      last processed address, new notifications start from lastAddress + 1
     * @throws NoSuchElementException   if any table of interest is never registered
     * @throws IllegalArgumentException if any table of interest is not opened before subscription
     */
    synchronized <K extends Message, V extends Message, M extends Message>
    void subscribe(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                   @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                   long lastAddress) {
        TableRegistry registry = runtime.getTableRegistry();
        List<TableSchema<K, V, M>> tableSchemas = tablesOfInterest
                .stream()
                .map(tName -> {
                    // The table should be opened full schema before subscription.
                    Table<K, V, M> table = registry.getTable(namespace, tName);
                    return new TableSchema<>(tName,
                            table.getKeyClass(), table.getValueClass(), table.getMetadataClass());
                })
                .collect(Collectors.toList());

        subscribe(streamListener, namespace, tableSchemas, lastAddress);
    }

    /**
     * Unsubscribe a prior subscription.
     *
     * @param streamListener Client listener.
     */
    void unsubscribe(@Nonnull StreamListener streamListener) {
        StreamSubscriber streamSubscriber = unsubscribeInternal(streamListener);
        if (streamSubscriber != null) {
            streamSubscriber.getScheduledFuture().cancel(false);
        }
    }

    /**
     * Internal method that can be safely invoked from the same thread executing the task.
     *
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
         * <p>
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
                       long lastAddress) {
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
            this.txnStream.seek(lastAddress + 1);
        }

        public void run() {
            while (!isShutdown.get()) {
                try {
                    Thread.currentThread().setName(namespace + listener.toString());
                    ILogData logData = txnStream.nextUpTo(Address.MAX);
                    if (logData == null) {
                        break; // Stream is all caught up, sleep for a bit.
                    }
                    // Avoid LogData decompression/deserialization if no table of interest contained.
                    if (Sets.intersection(logData.getStreams(), tablesOfInterest.keySet()).isEmpty()) {
                        continue;
                    }
                    MultiObjectSMREntry multiObjSMREntry = (MultiObjectSMREntry) logData.getPayload(runtime);
                    long epoch = logData.getEpoch();
                    Map<TableSchema, List<CorfuStreamEntry>> entries = new HashMap<>();
                    // first only filter by the stream IDs of interest that are present in this logData
                    logData.getStreams().stream().filter(tablesOfInterest::containsKey)
                            .forEach(streamId -> entries.put(tablesOfInterest.get(streamId),
                                    // Only extract the list of updates per stream as a list
                                    multiObjSMREntry.getSMRUpdates(streamId).stream().map(smrEntry ->
                                            CorfuStreamEntry.fromSMREntry(smrEntry, epoch)
                                    ).collect(Collectors.toList())));

                    if (!entries.isEmpty()) {
                        Timestamp timestamp = Timestamp.newBuilder()
                                .setSequence(logData.getGlobalAddress())
                                .setEpoch(epoch)
                                .build();
                        CorfuStreamEntries callbackResult = new CorfuStreamEntries(entries, timestamp);
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
