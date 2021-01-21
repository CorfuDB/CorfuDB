package org.corfudb.runtime.collections;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.ExecutorService;

/**
 * A runnable task for a subscription to retrieve the previously
 * polled transaction updates from the buffer and send notifications
 * to client via the registered call backs. This task is executed
 * by the thread pool continuously until an error occurs which would
 * stop the subscription.
 * <p>
 * Created by WenbinZhu on 11/9/20.
 */
@Slf4j
class StreamNotificationTask implements Runnable {

    // Number of transaction updates to send notification in each run.
    private static final int NOTIFICATION_BATCH_SIZE = 10;

    // Total amount of time to wait for retrieving the data changes from buffer if it is empty.
    private static final Duration QUEUE_EMPTY_BLOCK_TIME_MS = Duration.ofMillis(1_000);

    // A warning will be raised if client call back takes longer than this time threshold.
    private static final Duration SLOW_NOTIFICATION_TIME_MS = Duration.ofMillis(1_500);

    // The streaming manager that is in charge of listener subscriptions.
    private final StreamingManager streamingManager;

    // The subscription context associated with this task.
    private final StreamSubscription subscription;

    // The Thread pool for executing client notification tasks.
    private final ExecutorService notificationExecutor;

    StreamNotificationTask(StreamingManager streamingManager,
                           StreamSubscription subscription,
                           ExecutorService notificationExecutor) {
        this.streamingManager = streamingManager;
        this.subscription = subscription;
        this.notificationExecutor = notificationExecutor;
    }

    @Override
    public void run() {
        try {
            sendNotifications();
        } catch (Throwable throwable) {
            StreamListener listener = subscription.getListener();
            log.error("Encountered exception {} during client notification callback, " +
                    "listener: {}, namespace: {}", throwable, listener, subscription.getNamespace());
            streamingManager.unsubscribe(listener, false);
            listener.onError(throwable);
        }
    }

    /**
     * Retrieve the first data change from the buffer and send notification
     * to the client via the pre-registered call back.
     */
    private void sendNotifications() throws Exception {
        // Total amount of time for the batch waiting for buffer being not empty.
        long remainingBlockTime = QUEUE_EMPTY_BLOCK_TIME_MS.toNanos();
        StreamListener listener = subscription.getListener();

        for (int iter = 0; iter < NOTIFICATION_BATCH_SIZE; iter++) {
            // If listener already unsubscribed, do not process or schedule again.
            if (subscription.isStopped()) {
                return;
            }

            long startTime = System.nanoTime();
            CorfuStreamEntries nextUpdate = subscription.dequeueStreamEntry(remainingBlockTime);

            // No new updates after max waiting time elapses, break and re-schedule.
            if (nextUpdate == null) {
                break;
            }

            long endTime = System.nanoTime();
            remainingBlockTime -= endTime - startTime;

            // Send notification to client with the pre-registered callback.
            startTime = endTime;
            subscription.getStreamingMetrics().recordDeliveryDuration(() -> listener.onNext(nextUpdate));
            endTime = System.nanoTime();

            Duration onNextElapse = Duration.ofNanos(endTime - startTime);
            if (onNextElapse.compareTo(SLOW_NOTIFICATION_TIME_MS) > 0) {
                log.warn("Stream listener: {} onNext() took to long: {} ms.", listener, onNextElapse.toMillis());
            }
        }

        notificationExecutor.submit(this);
    }
}
