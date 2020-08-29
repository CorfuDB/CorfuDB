package org.corfudb.perf.streaming;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;

@Slf4j
public class Consumer extends Worker {

    /**
     * The period of time (in ms) in-between polls when there are no items to consume
     */
    private final long pollPeriodMs;

    public Consumer(final UUID id, final CorfuRuntime runtime,
                    final int numItems, final long pollPeriodMs) {
        super(id, runtime, numItems);
        this.pollPeriodMs = pollPeriodMs;
    }

    private void sleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(pollPeriodMs);
        } catch (InterruptedException ie) {
            log.error("Consumer[{}] interrupted", id);
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
    }

    @Override
    public void run() {
        log.debug("Consumer[{}] starting", id);
        final long startTime = System.currentTimeMillis();
        final IStreamView stream = runtime.getStreamsView().get(id);
        int totalItemsConsumed = 0;
        long totalBytesConsumed = 0;

        while (totalItemsConsumed < numItems) {

            // Keep consuming the stream until there's nothing left and then sleep
            // till there are more items to consume
            final ILogData item = stream.next();
            if (item == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Consumer[{}] polling, consumed {}", id, totalItemsConsumed);
                }
                sleep();
                continue;
            }

            final byte[] payload = (byte[]) item.getPayload(null);
            totalBytesConsumed += payload.length;
            totalItemsConsumed++;
        }

        final double totalTimeInSeconds =  (System.currentTimeMillis() - startTime * 1.0) / 10e3;
        log.debug("Consumer[{}] consumed {} items and {} bytes in {} seconds", id, numItems,
                totalBytesConsumed, totalTimeInSeconds);
    }
}
