package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.corfudb.runtime.CorfuRuntime;

/**
 *
 * Created by sneginhal on 10/22/2019.
 */
@Slf4j
public class TxnStreamingManager {

    /**
     * Corfu Runtime.
     */
    private final CorfuRuntime runtime;

    /**
     * Map of StreamingSubscriptionContexts.
     *
     * This is the map of all streaming subscriptions.
     * The key is is the hash code of the StreamListener. This allows multiple
     * subscriptions to the same namespace, table as long as the listeners
     * have a different hash code.
     */
    private final Map<Integer, StreamingSubscriptionContext> subscriptions = new HashMap<>();

    /**
     * Executor service to run the txn stream poller.
     */
    private ExecutorService pollerExecutor = Executors.newFixedThreadPool(1, (r) -> {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    t.setName("TxnStreamingManagerPoller");
                    return t;
                });

    /**
     * Executor service to run the notifier.
     */
    private ExecutorService notifierExecutor = Executors.newFixedThreadPool(3, (r) -> {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    t.setName("TxnStreamingManagerNotifier");
                    return t;
                });

    /**
     * Scheduled executor to run the periodic tasks.
     */
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor((r) -> {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    t.setName("TxnStreamingManagerScheduler");
                    return t;
                });

    /**
     * Interval of polling and notification in milliseconds.
     */
    private static final int INTERVAL = 50;

    /**
     * Maximum notifications to deliver in a run.
     */
    private static final int MAX_NOTIFICATIONS = 25;

    public TxnStreamingManager(@Nonnull CorfuRuntime runtime) {
        this.runtime = runtime;
        executor.scheduleWithFixedDelay(this::scheduleTxnPolling, 0, INTERVAL,
                TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(this::scheduleClientNotification, INTERVAL, INTERVAL,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Subscribe to updates.
     *
     * @param streamListener Client listsner.
     * @param namespace Namespace of interest.
     * @param tablesOfInterest Only updates from these tables will be returned.
     * @param startAddress Address to start the notifications from.
     */
    public synchronized <K extends Message, V extends Message, M extends Message>
            void subscribe(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                    @Nonnull List<TableSchema> tablesOfInterest, long startAddress) {

        if (subscriptions.containsKey(streamListener.hashCode())) {
            log.error("A stream listener {} with hash code {} already subscribed.",
                    streamListener.toString(), streamListener.hashCode());
            return;
        }

        StreamingSubscriptionContext sc = new StreamingSubscriptionContext(runtime, streamListener,
                namespace, tablesOfInterest, startAddress);

        subscriptions.put(streamListener.hashCode(), sc);

        log.info("Subscribed StreamListener {}", sc.toString());
    }

    /**
     * Unsubscribe a prior subscription.
     *
     * @param streamListener Client listsner.
     */
    public synchronized void unsubscribe(@Nonnull StreamListener streamListener) {
        StreamingSubscriptionContext sc = subscriptions.remove(streamListener.hashCode());
        if (sc != null) {
            log.info("Unsubscribed StreamListener {}", sc.toString());
        }
    }

    /**
     * Periodic task to schedule transaction polling.
     */
    private synchronized void scheduleTxnPolling() {
        // First remove any streamContexts that are ready to be reaped.
        List<Integer> keysToBeReaped = subscriptions.entrySet()
                .stream()
                .filter(e -> e.getValue().isReadyToBeReaped())
                .map(e -> e.getKey())
                .collect(Collectors.toList());
        keysToBeReaped.forEach(k -> subscriptions.remove(k));

        // Lock avaliable streamContexts.
        List<StreamingSubscriptionContext> lockedStreamingSubscriptionContexts = subscriptions.values()
                .stream()
                .filter(sc -> sc.acquire())
                .collect(Collectors.toList());

        if (!lockedStreamingSubscriptionContexts.isEmpty()) {
            log.trace("Locked {} StreamingSubscriptionContexts for processing",
                    lockedStreamingSubscriptionContexts.size());
            pollerExecutor.submit(new TransactionPoller(runtime, lockedStreamingSubscriptionContexts));
        }


    }

    /**
     * Periodic task to schedule client notifications.
     */
    private void scheduleClientNotification() {
        subscriptions.values().stream()
                .forEach(sc -> notifierExecutor.submit(() -> sc.notifyClient(MAX_NOTIFICATIONS)));
    }

    /**
     * Shutdown the TxnStreamingManager and all its threads.
     */
    public void shutdown() {
        Consumer<ExecutorService> shutdownExecutor = (executor) -> {
                    executor.shutdown();
                    try {
                        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                            executor.shutdownNow();
                        }
                    } catch (InterruptedException ie) {
                        executor.shutdownNow();
                    }
                };

        CompletableFuture.runAsync(() -> shutdownExecutor.accept(executor));
        CompletableFuture.runAsync(() -> shutdownExecutor.accept(pollerExecutor));
        CompletableFuture.runAsync(() -> shutdownExecutor.accept(notifierExecutor));
    }
}
