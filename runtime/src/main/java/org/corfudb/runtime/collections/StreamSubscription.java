package org.corfudb.runtime.collections;

import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import lombok.Getter;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.IStreamView;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The stream subscription context that stores stream listener,
 * table of interests and other relevant data structures.
 * <p>
 * Created by WenbinZhu on 11/5/20.
 */
public class StreamSubscription<K extends Message, V extends Message, M extends Message> {

    // Corfu runtime to interact with corfu streams.
    private final CorfuRuntime runtime;

    // Pre-registered client call back.
    @Getter
    private final StreamListener listener;

    // Namespace of interested tables.
    @Getter
    private final String namespace;

    // The corfu transaction stream to poll data changes from.
    @Getter
    private final IStreamView txnStream;

    // The table id to schema map of the interested tables.
    private final Map<UUID, TableSchema<K, V, M>> tableSchemas;

    // The buffer of polled transaction data changes.
    private final BlockingQueue<CorfuStreamQueueEntry> streamBuffer;

    // The size of the stream buffer.
    @Getter
    private final int streamBufferSize;

    // The listener id to tag metrics with.
    @Getter
    private final String listenerId;

    // Whether the subscription is stopped because of error or is unsubscribed.
    private volatile boolean stopped = false;

    public StreamSubscription(CorfuRuntime runtime, StreamListener listener, String namespace,
                       String streamTag, List<String> tablesOfInterest, int bufferSize) {
        this.runtime = runtime;
        this.listener = listener;
        this.namespace = namespace;
        this.streamBuffer = new ArrayBlockingQueue<>(bufferSize);
        this.streamBufferSize = bufferSize;
        this.listenerId = String.format("listener_%s_%s_%s", listener, namespace, streamTag);

        // Generate table name to table schema mapping.
        TableRegistry registry = runtime.getTableRegistry();
        UUID txnStreamId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);
        this.tableSchemas = tablesOfInterest
                .stream()
                .collect(Collectors.toMap(
                        tName -> CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(namespace, tName)),
                        tName -> {
                            // The table should be opened with full schema before subscription.
                            Table<K, V, M> t = registry.getTable(namespace, tName);
                            if (!t.getStreamTags().contains(txnStreamId)) {
                                throw new IllegalArgumentException(String.format("Interested table: %s does not " +
                                        "have specified stream tag: %s", t.getFullyQualifiedTableName(), streamTag));
                            }
                            return new TableSchema<>(tName, t.getKeyClass(), t.getValueClass(), t.getMetadataClass());
                        }));

        // Create transaction stream with regard to the stream tag.
        StreamOptions options = StreamOptions
                .builder()
                .cacheEntries(false)
                .isCheckpointCapable(false)
                .build();
        this.txnStream = runtime.getStreamsView().get(txnStreamId, options);
    }

    /**
     * Enqueue the polled transaction data into the buffer, block until the buffer
     * queue is not full or the specified waiting time elapses.
     *
     * @param logData      the polled transaction data
     * @param maxBlockTime maximum time waiting if buffer is full
     * @return true if successfully enqueues the data, false otherwise
     * @throws InterruptedException if interrupted while waiting
     */
    boolean enqueueStreamEntry(ILogData logData, long maxBlockTime) throws InterruptedException {
        // Avoid LogData de-compression if it does not contain any table of interest.
        if (Sets.intersection(logData.getStreams(), tableSchemas.keySet()).isEmpty()) {
            return true;
        }

        long epoch = logData.getEpoch();
        MultiObjectSMREntry smrEntries = (MultiObjectSMREntry) logData.getPayload(runtime);
        Map<TableSchema, List<CorfuStreamEntry>> streamEntries = new HashMap<>();

        Map<UUID, TableSchema<K, V, M>> filteredSchemas = logData.getStreams()
                .stream()
                .filter(tableSchemas::containsKey)
                .collect(Collectors.toMap(Function.identity(), tableSchemas::get));

        filteredSchemas.forEach((streamId, schema) -> {
            // Only deserialize the interested streams to reduce overheads.
            List<CorfuStreamEntry> entryList = smrEntries.getSMRUpdates(streamId)
                    .stream()
                    .map(entry -> CorfuStreamEntry.fromSMREntry(entry, epoch))
                    .collect(Collectors.toList());
            if (!entryList.isEmpty()) {
                streamEntries.put(schema, entryList);
            }
        });

        // This transaction data does not contain any table of interest, don't add to buffer.
        if (streamEntries.isEmpty()) {
            return true;
        }

        Timestamp timestamp = Timestamp.newBuilder()
                .setSequence(logData.getGlobalAddress())
                .setEpoch(epoch)
                .build();

        CorfuStreamEntries entries = new CorfuStreamEntries(streamEntries, timestamp);
        return streamBuffer.offer(new CorfuStreamQueueEntry(entries, System.nanoTime()), maxBlockTime, TimeUnit.NANOSECONDS);
    }

    /**
     * Dequeue the first available transaction update, block until the buffer
     * queue is not empty or the specified waiting time elapses.
     *
     * @param maxBlockTime maximum time waiting if buffer is empty
     * @return entry if successfully dequeue the first update, null otherwise
     * @throws InterruptedException if interrupted while waiting
     */
    @Nullable
    CorfuStreamQueueEntry dequeueStreamEntry(long maxBlockTime) throws InterruptedException {
        return streamBuffer.poll(maxBlockTime, TimeUnit.NANOSECONDS);
    }

    /**
     * Stop the subscription.
     */
    public void stop() {
        stopped = true;
    }

    /**
     * Get whether the subscription is stopped.
     *
     * @return true if subscription is stopped, false otherwise
     */
    public boolean isStopped() {
        return stopped;
    }
}
