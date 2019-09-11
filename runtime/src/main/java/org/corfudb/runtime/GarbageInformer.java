package org.corfudb.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMRGarbageEntry;
import org.corfudb.protocols.logprotocol.SMRGarbageRecord;
import org.corfudb.protocols.logprotocol.SMRRecordLocator;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Layout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GarbageInformer {

    private final static Duration GC_PERIOD = Duration.ofSeconds(15);
    private final static int RECEIVING_QUEUE_CAPACITY = 5_000;
    private final static int SENDING_QUEUE_CAPACITY = 20;
    private final static int BATCH_SIZE = 100;

    private final CorfuRuntime rt;

    // executor to drain garbageReceivingQueue when it is full.
    private final ExecutorService drainExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("GarbageInformerDrain")
                    .build());

    /**
     * The queue to receive single garbage decisions from ObjectView
     */
    @Getter
    private final BlockingQueue<SMRGarbageEntry> garbageReceivingQueue =
            new LinkedBlockingQueue<>(RECEIVING_QUEUE_CAPACITY);

    /**
     * The queue to sending merged garbage decision to LogUnit servers
     */
    @Getter
    private final BlockingDeque<GarbageBatch> garbageSendingDeque =
            new LinkedBlockingDeque<>(SENDING_QUEUE_CAPACITY);

    private final ScheduledExecutorService gcScheduler = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("GarbageInformerGC")
                    .build());

    /**
     * Constructor
     *
     * @param rt Corfu runtime
     */
    public GarbageInformer(CorfuRuntime rt) {
        this.rt = rt;
    }

    /**
     * Start to send garbage decisions to LogUnit servers.
     */
    public void start() {
        Random rand = new Random();

        // periodically to drain garbageReceivingQueue and send garbage decisions to LogUnit servers.
        // Randomized initial delay prevents all runtime send garbage decision simultaneously.
        gcScheduler.scheduleWithFixedDelay(this::gc,
                GC_PERIOD.getSeconds() + rand.nextInt((int) GC_PERIOD.getSeconds()),
                GC_PERIOD.getSeconds(),
                TimeUnit.SECONDS);
    }

    /**
     * Stop to send garbage decisions to LogUnit servers.
     * This function shuts down the single-threaded executor.
     */
    public void stop() {
        gcScheduler.shutdownNow();
    }

    /**
     * Adds a list of SMRRecordLocators whose associated SMRRecords are marked as garbage by the same global address.
     *
     * @param markerAddress The global address marks the garbage.
     * @param locators      A list of locators whose associated SMRRecords are marked as garbage.
     */
    public void add(long markerAddress, List<SMRRecordLocator> locators) {
        // sanity check
        if (locators.isEmpty()) {
            return;
        }

        List<SMRGarbageEntry> garbageEntries = generateGarbageEntries(markerAddress, locators);

        // synchronization to prevent concurrent access to garbageReceivingQueue to maintain order by marker address.
        synchronized (this) {
            try {
                for (SMRGarbageEntry garbageEntry : garbageEntries) {
                    boolean success = garbageReceivingQueue.offer(garbageEntry);
                    if (!success) {
                        drainExecutor.submit(this::gc);
                        garbageReceivingQueue.put(garbageEntry);
                    }
                }
            } catch (InterruptedException ie) {
                throw new UnrecoverableCorfuInterruptedError(
                        "Interrupted during adding locators to GarbageInformer", ie);
            }
        }
    }

    private List<SMRGarbageEntry> generateGarbageEntries(long markerAddress,
                                                         List<SMRRecordLocator> locators) {
        Map<Long, SMRGarbageEntry> garbage = new HashMap<>();

        locators.forEach(locator -> {
            int serializedSize = locator.getSerializedSize();
            SMRGarbageRecord garbageRecord = new SMRGarbageRecord(markerAddress, serializedSize);
            long globalAddress = locator.getGlobalAddress();

            garbage.compute(globalAddress, (a, smrGarbageEntry) -> {
                if (smrGarbageEntry == null) {
                    smrGarbageEntry = new SMRGarbageEntry();
                    smrGarbageEntry.setGlobalAddress(a);
                }
                smrGarbageEntry.add(locator.getStreamId(), locator.getIndex(), garbageRecord);
                return smrGarbageEntry;
            });
        });

        return new ArrayList<>(garbage.values());
    }

    /**
     * Drains garbage decisions from receiving queue and sends them to LogUnit servers.
     */
    public synchronized void gc() {
        // drains sending queue first when it reaches the capacity limit.
        if (garbageSendingDeque.size() >= SENDING_QUEUE_CAPACITY) {
            log.debug("GarbageInformer: Drains sending queue");
            sendGarbage();
        }

        // If garbageSendingDeque is still full, gives up drain garbageReceivingQueue.
        if (garbageSendingDeque.size() >= SENDING_QUEUE_CAPACITY) {
            log.debug("GarbageInformer: Stop drains receiving queue because sending queue is full");
            return;
        }

        // TODO(xin): fill lastMarker in future commits.
        Map<UUID, Long> lastMarkers = new HashMap<>();

        Map<Long, SMRGarbageEntry> addressToGarbage = new HashMap<>();

        List<SMRGarbageEntry> garbageEntries = new ArrayList<>();
        garbageReceivingQueue.drainTo(garbageEntries, BATCH_SIZE);

        if (garbageEntries.isEmpty()) {
            log.trace("Garbage Informer has nothing to send");
            return;
        }

        for (SMRGarbageEntry garbageEntry : garbageEntries) {
            long globalAddress = garbageEntry.getGlobalAddress();

            if (!addressToGarbage.containsKey(globalAddress)) {
                addressToGarbage.put(globalAddress, garbageEntry);
            } else {
                addressToGarbage.get(globalAddress).merge(garbageEntry);
            }
        }

        GarbageBatch garbageBatch = new GarbageBatch(addressToGarbage.values(), lastMarkers);
        garbageSendingDeque.offer(garbageBatch);
        sendGarbage();
    }

    private void sendGarbage() {
        GarbageBatch garbageBatch = null;
        try {
            while ((garbageBatch = garbageSendingDeque.poll()) != null) {
                sendGarbageBatch(garbageBatch);
            }
        } catch (Exception e) {
            log.error("GarbageInformer: Caught exception in the write processor.", e);
            // Adds pending garbage to the head of garbageSendingDeque and waits for another
            // cycle to send garbage to LogUnit servers.
            if (garbageBatch != null) {
                garbageSendingDeque.addFirst(garbageBatch);
            }
        }
    }

    /**
     * Sends GarbageEntry batch as well as last marker address to LogUnits.
     */
    @VisibleForTesting
    public void sendGarbageBatch(GarbageBatch batch) {
        rt.getAddressSpaceView().layoutHelper(e -> {
            Layout layout = e.getLayout();

            // Assume the number of stripes of each stripe is equal and the order of stripes remains unchanged.
            Map<Integer, List<SMRGarbageEntry>> stripIndexToGarbageEntries = new HashMap<>();

            // shard SMRGarbageEntries based on stripe.
            batch.getGarbageEntries().forEach(garbageEntry -> {
                long globalAddress = garbageEntry.getGlobalAddress();
                int stripeIndex = layout.getStripeIndex(globalAddress);
                stripIndexToGarbageEntries.computeIfAbsent(stripeIndex, s -> new ArrayList<>()).add(garbageEntry);
            });

            // send GarbageEntry batch.
            stripIndexToGarbageEntries.forEach((stripeIndex, garbageEntries) ->
                    rt.getAddressSpaceView().sparseTrim(rt.getLayoutView().getRuntimeLayout(), stripeIndex,
                            garbageEntries)
            );

            // TODO(xin): Inform LogUnits about the last markers.
            return null;
        }, true);
    }

    /**
     * Contains a batch of garbageEntries as well as the last marker address.
     * The batch of garbageEntries and the marker address are sent to LogUnit
     * servers atomically, i.e. if any of them is failed to send to the LogUnits,
     * all of them are sent again.
     */
    @Data
    public static class GarbageBatch {
        final Collection<SMRGarbageEntry> garbageEntries;
        final Map<UUID, Long> lastMarkers;
    }
}

