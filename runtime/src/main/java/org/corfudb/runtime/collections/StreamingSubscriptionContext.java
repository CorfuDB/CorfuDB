package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;

/**
 * This is the context of one client of the streaming API.
 *
 * Created by sneginhal on 10/22/2019.
 */
@Slf4j
public class StreamingSubscriptionContext {

    /**
     * StreamListener.
     */
    @Getter
    private final StreamListener listener;

    /**
     * Namespace of interest.
     */
    @Getter
    private final String namespace;

    /**
     * Tables of interest.
     *
     * Map of (Stream Id - TableSchema)
     */
    @Getter
    private final Map<UUID,
        TableSchema<? extends Message, ? extends Message, ? extends Message>> tablesOfInterest;

    /**
     * Starting address.
     */
    private final long startAddress;

    /**
     * Last read address of the stream.
     */
    @Getter
    private long lastReadAddress;

    /**
     * A flag to indicate if a TransactionPoller is processing this StreamingSubscriptionContext.
     */
    private final AtomicBoolean txPollerInProgress = new AtomicBoolean(false);

    /**
     * Exception encountered when polling for this StreamingSubscriptionContext.
     */
    @Getter
    @Setter
    private volatile Throwable pollerException = null;

    /**
     * Is the StreamingSubscriptionContext ready to be reaped.
     *
     * After an exception has been delivered to the client, the StreamingSubscriptionContext is
     * no longer active and can be reaped.
     */
    @Getter
    private volatile boolean readyToBeReaped = false;

    /**
     * Maximum size of the streamQueue.
     */
    private final static int QUEUE_CAPACITY = 512;

    /**
     * Second defined in nano
     */
    /**
     * Threshold for long running callbacks in seconds
     */
    private final static long thresholdForLongRunningCallsInNano = 60L * 1_000_000_000;
    /**
     * Bounded queue of CorfuStreamEntries to be delivered to the client.
     */
    private final LinkedBlockingQueue<CorfuStreamEntries> streamQueue =
            new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    /**
     * A flag to indicate if a notification thread is already running for this
     * StreamingSubscriptionContext.
     */
    private final AtomicBoolean notificationInProgress = new AtomicBoolean(false);

    /**
     * Constructor.
     *
     * @param listener The client listener.
     * @param namespace The namespace of interest.
     * @param tableSchemas Only updates from these tables will be returned.
     * @param startAddress The starting address from which all updates are to be streamed.
     */
    public StreamingSubscriptionContext(@Nonnull CorfuRuntime runtime, @Nonnull StreamListener listener,
            @Nonnull String namespace, @Nonnull List<TableSchema> tableSchemas,
            long startAddress) {
        this.listener = listener;
        this.namespace = namespace;
        this.tablesOfInterest = tableSchemas.stream().collect(Collectors.toMap(
                        e -> runtime.getStreamID(runtime.getTableRegistry()
                                .getFullyQualifiedTableName(namespace, e.getTableName())),
                        e -> e));
        this.startAddress = startAddress;
        this.lastReadAddress = startAddress;
    }

    /**
     * Acquire the StreamingSubscriptionContext for polling.
     */
    public boolean acquire() {
        // If a TransactionPoller is already working on this StreamingSubscriptionContext or an
        // exception was encountered when polling return false.
        if (pollerException != null) {
            return false;
        }

        return txPollerInProgress.compareAndSet(false, true);
    }

    /**
     * Release the StreamingSubscriptionContext.
     */
    public void release() {
        txPollerInProgress.set(false);
    }

    /**
     * Enqueue the next stream entry.
     *
     * @param lastAddress Address of the last stream entry.
     * @param logData ILogData of the next stream entry.
     */
    public boolean enqueueStreamEntry(long lastAddress, ILogData logData) {
        // Check if the logData contains the any of the streams of interest.
        // If it does not move the lastReadAddress to indicate that this
        // address has been processed.
        Set<UUID> logDataStreams = new HashSet(logData.getStreams());
        logDataStreams.retainAll(tablesOfInterest.keySet());

        if (logDataStreams.isEmpty()) {
            log.trace("Entry at address {} does not contain any streams of interest to listener {}",
                    logData.getGlobalAddress(), listener.toString());
            lastReadAddress = logData.getGlobalAddress();
            return true;
        }

        synchronized (this) {
            // If the lastAddress is not the same as lastReadAddress, something went wrong.
            // Maybe multiple threads are trying to enqueue to the same StreamingSubscriptionContext.
            if (lastReadAddress != lastAddress) {
                log.error("Incorrect address given to enqueueStreamEntry. Given {} Expected {}",
                        lastAddress, lastReadAddress);
                return false;
            }

            MultiObjectSMREntry multiObjSMREntry = (MultiObjectSMREntry) logData.getPayload();
            long epoch = logData.getEpoch();
            //Build the CorfuStreamEntries from the MultiObjectSMREntry.
            CorfuStreamEntries update = new CorfuStreamEntries(multiObjSMREntry.getEntryMap()
                    .entrySet()
                    .stream()
                    .filter(e -> tablesOfInterest.containsKey(e.getKey()))
                    .collect(Collectors.toMap(
                        e -> tablesOfInterest.get(e.getKey()),
                        e -> e.getValue().getUpdates()
                                .stream()
                                .map(smrEntry -> CorfuStreamEntry.fromSMREntry(smrEntry,
                                            epoch,
                                            tablesOfInterest.get(e.getKey()).getKeyClass(),
                                            tablesOfInterest.get(e.getKey()).getPayloadClass(),
                                            tablesOfInterest.get(e.getKey()).getMetadataClass()))
                                .collect(Collectors.toList()))));

            // Now enqueue the update. The enqueue can fail if the queue has reached its capacity.
            // Update the lastReadAddress iff the enqueue is successful.
            if (streamQueue.offer(update)) {
                lastReadAddress = multiObjSMREntry.getGlobalAddress();
                return true;
            }
        }
        return false;
    }

    /**
     * Deliver updates to the client.
     *
     * @param numUpdatesToDeliver Number of updates to deliver.
     */
    public void notifyClient(int numUpdatesToDeliver) {
        if (notificationInProgress.compareAndSet(false, true)) {
            int updatesDelivered = 0;
            try {
                log.trace("Starting notification for {}", listener.toString());
                do {
                    // If an exception was encountered when polling for updates, notify the client
                    // of the error.
                    if (pollerException != null) {
                        // If the StreamingSubscriptionContext is marked readyToBeReaped, the error
                        // has already been delivered to the client. If not, deliver the exception
                        // and mark the StreamingSubscriptionContext ready for reaping.
                        if (!readyToBeReaped) {
                            log.warn("Encountered {} when streaming updates on namespace {}, " +
                                    "{} for {}", pollerException, namespace, listener.toString());
                            listener.onError(pollerException);
                            log.trace("Throwable {} delivered to {}", pollerException,
                                    listener.toString());
                            readyToBeReaped = true;
                        }
                        // The StreamingSubscriptionContext will be shortly reaped, no need to
                        // deliver any more updates on this stream.
                        streamQueue.clear();
                        return;
                    }

                    CorfuStreamEntries nextUpdate = streamQueue.poll();
                    if (nextUpdate == null) {
                        return;
                    }
                    long before = System.nanoTime();

                    listener.onNext(nextUpdate);

                    long after = System.nanoTime();
                    if (after - before > thresholdForLongRunningCallsInNano) {
                        log.error("{} onNext() took too long {} seconds", listener.toString(),
                                (after - before)/1_000_000_000);
                    }
                    updatesDelivered++;
                } while (numUpdatesToDeliver > updatesDelivered);
            } catch (Throwable t) {
                // Catch all possible exceptions, we don't want to loose a thread on the account of
                // the client code throwing an exception.
                log.error("Aborting client notification for {} due to an exception {}",
                        listener.toString(), t);
            } finally {
                log.trace("End of notification for {}, delivered {} updates", listener.toString(),
                        updatesDelivered);
                notificationInProgress.set(false);
            }
        } else {
            log.trace("Another notification thread is already servicing this " +
                    "StreamingSubscriptionContext {}", listener.toString());
        }
    }

    public boolean equals(StreamingSubscriptionContext sc) {
        return listener.equals(sc.getListener());
    }

    public int hashCode() {
        return listener.hashCode();
    }

    public String toString() {
        return listener.toString() + "(" + namespace + ")";
    }
}
