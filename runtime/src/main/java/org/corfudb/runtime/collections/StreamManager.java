package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

    private static final int MAX_SUBSCRIBERS = 12; // Arbitrary sanity check value.

    /**
     * Corfu Runtime.
     */
    @Getter
    private final CorfuRuntime runtime;

    /**
     * Map of StreamingSubscriptionThreads.
     * <p>
     * This is the map of all streaming subscriptions.
     */
    @Getter
    private final Map<StreamListener, SubscriptionThread> subscriptions = new HashMap<>();

    public StreamManager(@Nonnull CorfuRuntime runtime) {
        this.runtime = runtime;
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
                   @Nonnull List<TableSchema> tablesOfInterest, long startAddress) {
        if (subscriptions.size() >= MAX_SUBSCRIBERS) {
            throw new StreamSubscriptionException(
                    "subscribe: Max " + MAX_SUBSCRIBERS + " hit! Consider consolidating "
                            + streamListener);
        }
        if (subscriptions.containsKey(streamListener)) {
            throw new StreamSubscriptionException(
                    "StreamManager:subscriber already registered "
                            + streamListener + ". Maybe call unregister first?");
        }
        log.info("StreamManager::subscribe {}, startAddress {}, namespace {}, tables {}",
                streamListener, startAddress, namespace, tablesOfInterest.toString());
        SubscriptionThread st = new SubscriptionThread(this, streamListener,
                namespace, tablesOfInterest, startAddress);
        subscriptions.put(streamListener, st);
        st.start();
    }

    /**
     * Unsubscribe a prior subscription.
     *
     * @param streamListener Client listener.
     */
    public synchronized void unsubscribe(@Nonnull StreamListener streamListener) {
        SubscriptionThread st = subscriptions.remove(streamListener);
        if (st == null) {
            log.warn("StreamManager::unsubscribe has no context for {}", streamListener.toString());
            return;
        }
        st.shutdown.set(true);
        st.interrupt();
        log.info("Unsubscribed StreamListener {}", streamListener.toString());
    }

    class SubscriptionThread extends Thread {
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

        public AtomicBoolean shutdown;

        /**
         * A reference to the underlying stream
         */
        @Getter
        private final IStreamView txnStream;

        public void run() {
            try {
                log.info("{}:Seeking txStream to {}", listener.toString(), startAddress + 1);
                txnStream.seek(startAddress + 1);
            } catch (Throwable throwable) {
                log.warn("{}:onError at seek to {}", listener.toString(), startAddress + 1);
                listener.onError(throwable);
                streamManager.unsubscribe(listener);
                return;
            }

            try {
                while (!shutdown.get()) {
                    while (!txnStream.hasNext()) {
                        log.trace("{}::run() all caught up. Taking a 50ms nap", listener.toString());
                        final int defaultTimeoutInMilli = 50;
                        TimeUnit.MILLISECONDS.sleep(defaultTimeoutInMilli);
                    }
                    ILogData logData = txnStream.nextUpTo(Address.MAX);
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
                        log.debug("{}::onNext with {} updates", listener.toString(), entries.size());
                        long onNextStart = System.nanoTime();
                        listener.onNext(callbackResult);
                        long onNextEnd = System.nanoTime();
                        if (TimeUnit.NANOSECONDS.toSeconds(onNextEnd - onNextStart) > 0) {
                            log.debug("{}::onNext took {}s", listener.toString(),
                                    TimeUnit.NANOSECONDS.toSeconds(onNextEnd - onNextStart));
                        }
                    }
                }
            } catch (Throwable throwable) {
                log.warn("{}::onError: {}", listener.toString(), throwable.toString());
                listener.onError(throwable);
                streamManager.unsubscribe(listener);
            }
        }

        public SubscriptionThread(@Nonnull StreamManager streamManager,
                                  @Nonnull StreamListener listener,
                                  @Nonnull String namespace,
                                  @Nonnull List<TableSchema> tableSchemas,
                                  long startAddress) {
            super(String.format("CorfuSubscriber: {}:{}", listener.toString(), namespace));
            this.streamManager = streamManager;
            this.listener = listener;
            this.namespace = namespace;
            this.tablesOfInterest = new HashMap<>();
            this.shutdown = new AtomicBoolean();
            tableSchemas.stream().forEach(tableSchema -> {
                tablesOfInterest.put(
                        CorfuRuntime.getStreamID(
                                TableRegistry.getFullyQualifiedTableName(namespace,
                                        tableSchema.getTableName())
                        ), tableSchema
                );
            });

            this.startAddress = startAddress;
            StreamOptions options = StreamOptions.builder()
                    .cacheEntries(false)
                    .build();

            this.txnStream = runtime.getStreamsView()
                    .get(ObjectsView.TRANSACTION_STREAM_ID, options);
        }
    }
}
