package org.corfudb.runtime.collections;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.stream.IStreamView;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A runnable task for a subscription to poll data change from the
 * transaction stream and put into the subscription's buffer. This
 * task is executed by the thread pool continuously until an error
 * occurs which would stop the subscription.
 * <p>
 * Created by WenbinZhu on 11/9/20.
 */
@Slf4j
class StreamPollingTask implements Runnable {

    // A period of time in ms to sleep before next cycle when poller gets no new data changes.
    private static final long IDLE_WAIT_TIME_MS = 50;

    // Total amount of time to wait for putting the polled data changes into buffer if it is full.
    private static final Duration QUEUE_FULL_BLOCK_TIME_MS = Duration.ofMillis(1_000);

    // The streaming manager that is in charge of listener subscriptions.
    private final StreamingManager streamingManager;

    // The corfu transaction stream to poll data changes from.
    private final IStreamView txnStream;

    // The subscription context associated with this task.
    private final StreamSubscription subscription;

    // The Thread pool for executing stream polling tasks.
    private final ScheduledExecutorService pollingExecutor;

    // Last address of the data successfully processed by the buffer.
    private long lastReadAddress;

    StreamPollingTask(StreamingManager streamingManager, long lastAddress,
                      StreamSubscription subscription, ScheduledExecutorService executor) {
        this.streamingManager = streamingManager;
        this.subscription = subscription;
        this.pollingExecutor = executor;
        this.lastReadAddress = lastAddress;
        this.txnStream = subscription.getTxnStream();
    }

    @Override
    public void run() {
        try {
            pollTxnStream();
        } catch (TrimmedException te) {
            processException(new StreamingException(te));
        } catch (Throwable throwable) {
            processException(throwable);
        }
    }

    /**
     * Unsubscribe listener and notify with error.
     *
     * @param throwable
     */
    private void processException(Throwable throwable) {
        StreamListener listener = subscription.getListener();
        log.error("Encountered exception {} during txn stream polling, listener: {}, " +
                "namespace: {}", throwable, listener, subscription.getNamespace());
        streamingManager.unsubscribe(listener, false);
        listener.onError(throwable);
    }

    /**
     * Poll new data changes from the transaction stream and put into
     * the subscription's buffer one by one.
     */
    private void pollTxnStream() throws Exception {
        // If listener already unsubscribed, do not process or schedule again.
        if (subscription.isStopped()) {
            return;
        }

        // Seek to next address and poll transaction updates.
        txnStream.seek(lastReadAddress + 1L);
        List<ILogData> updates = subscription.getStreamingMetrics().recordPollingDuration(
                () -> txnStream.remainingAtMost(subscription.getStreamBufferSize()));

        // No new updates, take a short break and poll again.
        if (updates.isEmpty()) {
            pollingExecutor.schedule(this, IDLE_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
            return;
        }

        // Insert polled updates to the subscription buffer, with a shared
        // fixed amount of time waiting for buffer being not full.
        long remainingBlockTime = QUEUE_FULL_BLOCK_TIME_MS.toNanos();
        for (ILogData update : updates) {
            if (subscription.isStopped()) {
                return;
            }

            // Buffer is full after max waiting time elapses, break and re-schedule.
            long startTime = System.nanoTime();
            if (!subscription.enqueueStreamEntry(update, remainingBlockTime)) {
                break;
            }
            remainingBlockTime -= System.nanoTime() - startTime;

            // Sanity check to ensure lastReadAddress never regress.
            long updateAddress = update.getGlobalAddress();
            if (updateAddress <= lastReadAddress) {
                throw new IllegalStateException(String.format(
                        "lastReadAddress regressing from %d to %d", lastReadAddress, updateAddress));
            }
            lastReadAddress = updateAddress;
        }

        // Re-submit itself to the executor so polling will start again.
        pollingExecutor.submit(this);
    }
}
