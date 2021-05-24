package org.corfudb.runtime.collections;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuRuntime;

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

    // A warning will be raised if client call back takes longer than this time threshold.
    private static final Duration SLOW_NOTIFICATION_TIME_MS = Duration.ofMillis(1_500);

    // The streaming manager that is in charge of listener subscriptions.
    private final StreamingManager streamingManager;

    // The subscription context associated with this task.
    private final StreamSubscription subscription;

    // The Thread pool for executing client notification tasks.
    private final ExecutorService notificationExecutor;

    // Runtime parameters
    private final CorfuRuntime.CorfuRuntimeParameters parameters;

    StreamNotificationTask(StreamingManager streamingManager,
                           StreamSubscription subscription,
                           ExecutorService notificationExecutor, CorfuRuntime.CorfuRuntimeParameters parameters) {
        this.streamingManager = streamingManager;
        this.subscription = subscription;
        this.notificationExecutor = notificationExecutor;
        this.parameters = parameters;
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
        // Total time in milliseconds to block waiting for new updates to appear in the queue, if empty.
        long remainingBlockTime = Duration.ofMillis(parameters.getStreamingNotificationBlockingTimeMs()).toNanos();

        StreamListener listener = subscription.getListener();

        for (int iter = 0; iter < parameters.getStreamingNotificationBatchSize(); iter++) {
            // If listener already unsubscribed, do not process or schedule again.
            if (subscription.isStopped()) {
                return;
            }

            long startTime = System.nanoTime();
            CorfuStreamQueueEntry queueEntry = subscription.dequeueStreamEntry(remainingBlockTime);

            // No new updates after max waiting time elapses, break and re-schedule.
            if (queueEntry == null) {
                break;
            }
            String listenerId = subscription.getListenerId();
            MicroMeterUtils.time(Duration.ofNanos(System.nanoTime() - queueEntry.getEnqueueTime()),
                    "stream_sub.queueDuration.timer", "listener", listenerId);
            CorfuStreamEntries nextUpdate = queueEntry.getEntry();

            long endTime = System.nanoTime();
            remainingBlockTime -= endTime - startTime;

            // Send notification to client with the pre-registered callback.
            startTime = endTime;

            MicroMeterUtils.time(() -> listener.onNext(nextUpdate),
                    "stream.notify.duration",
                    "listener",
                    listenerId);
            endTime = System.nanoTime();

            Duration onNextElapse = Duration.ofNanos(endTime - startTime);
            if (onNextElapse.compareTo(SLOW_NOTIFICATION_TIME_MS) > 0) {
                log.warn("Stream listener: {} onNext() took too long: {} ms.", listener, onNextElapse.toMillis());
            }
        }

        notificationExecutor.submit(this);
    }
}
