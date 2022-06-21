package org.corfudb.runtime.collections.streaming;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * This class represents a Streaming task that is managed by {@link StreamPollingScheduler}. It binds a stream listener
 * {@link StreamListener} to a {@link DeltaStream}, every time it's scheduled to sync, it will read data for a specific
 * stream tag, transform it and propagate it to the listener.
 *
 * @param <K> - type of the protobuf KeySchema defined while the table was created.
 * @param <V> - type of the protobuf PayloadSchema defined by the table creator.
 * @param <M> - type of the protobuf metadata schema defined by table creation.
 *
 */

@Slf4j
public class StreamingTask<K extends Message, V extends Message, M extends Message> implements Runnable {

    // Pre-registered client call back.
    @Getter
    private final StreamListener listener;

    // The table id to schema map of the interested tables.
    private final Map<UUID, TableSchema<K, V, M>> tableSchemas;

    @Getter
    private final String listenerId;

    private final CorfuRuntime runtime;

    private final ExecutorService workerPool;

    private final AtomicReference<StreamStatus> status;

    private volatile Throwable error;

    private final List<DeltaStream> streamsList;

    private DeltaStream currentStream = null;

    public StreamingTask(CorfuRuntime runtime, ExecutorService workerPool,
                         @Nonnull Map<String, String> nsToStreamTags, StreamListener listener,
                         Map<String, List<String>> nsToTables,
                         long address,
                         int bufferSize) {

        String appNamespace;
        // nsToStreamTags.size > 1 only when the listener is across clientNamespace and corfu namespace.
        // Retrieve appNamespace to form the listenerID
        if (nsToStreamTags.size() > 1) {
            appNamespace = nsToStreamTags.keySet().stream()
                    .filter(ns -> ns != CORFU_SYSTEM_NAMESPACE).findFirst().get();
        } else {
            appNamespace = nsToStreamTags.keySet().stream().findFirst().get();
        }
        String appStreamTag = nsToStreamTags.get(appNamespace);

        this.runtime = runtime;
        this.workerPool = workerPool;
        this.listenerId = String.format("listener_%s_%s_%s", listener, appNamespace, appStreamTag);
        this.listener = listener;
        TableRegistry registry = runtime.getTableRegistry();
        this.streamsList = new ArrayList<>();

        nsToStreamTags.entrySet().stream().forEach(nsToTag -> {
            final UUID streamId = TableRegistry.getStreamIdForStreamTag(nsToTag.getKey(), nsToTag.getValue());
            DeltaStream stream = new DeltaStream(runtime.getAddressSpaceView(), streamId, address, bufferSize);
            streamsList.add(stream);
        });

        this.tableSchemas = new HashMap<>();
        for (Map.Entry<String, List<String>> nsToTablesEntry : nsToTables.entrySet()) {
            for (String table : nsToTablesEntry.getValue()) {
                UUID streamId = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(
                        nsToTablesEntry.getKey(), table));
                Table<K, V, M> t = registry.getTable(nsToTablesEntry.getKey(), table);
                String streamTag = nsToStreamTags.get(nsToTablesEntry.getKey());
                UUID streamTagId = TableRegistry.getStreamIdForStreamTag(nsToTablesEntry.getKey(),
                        streamTag);
                if (!t.getStreamTags().contains(streamTagId)) {
                    throw new IllegalArgumentException(String.format("Interested table: %s does not " +
                            "have specified stream tag: %s", t.getFullyQualifiedTableName(), streamTag));
                }
                tableSchemas.put(streamId, new TableSchema<>(table, t.getKeyClass(),
                        t.getValueClass(), t.getMetadataClass()));
            }
        }

        this.status = new AtomicReference<>(StreamStatus.RUNNABLE);
    }

    public StreamStatus getStatus() {
        return status.get();
    }

    public void move(StreamStatus from, StreamStatus to) {
        Preconditions.checkState(status.compareAndSet(from, to),
                "move: failed to change %s to %s", from, to);
    }

    private Optional<CorfuStreamEntries> transform(ILogData logData) {
        Objects.requireNonNull(logData);
        // Avoid LogData de-compression if it does not contain any table of interest.
        if (logData.isHole() || Sets.intersection(logData.getStreams(), tableSchemas.keySet()).isEmpty()) {
            return Optional.empty();
        }

        long epoch = logData.getEpoch();
        MultiObjectSMREntry smrEntries = (MultiObjectSMREntry) logData.getPayload(runtime);

        Map<UUID, TableSchema<K, V, M>> filteredSchemas = logData.getStreams()
                .stream()
                .filter(tableSchemas::containsKey)
                .collect(Collectors.toMap(Function.identity(), tableSchemas::get));

        Map<TableSchema, List<CorfuStreamEntry>> streamEntries = new HashMap<>();

        filteredSchemas.forEach((streamId, schema) -> {
            // Only deserialize the interested streams to reduce overheads.
            List<CorfuStreamEntry> entryList = smrEntries.getSMRUpdates(streamId)
                    .stream()
                    .map(CorfuStreamEntry::fromSMREntry)
                    .collect(Collectors.toList());

            // Deduplicate entries per stream Id, ordering within a transaction is not guaranteed
            Map<Message, CorfuStreamEntry> observedKeys = new HashMap<>();
            entryList.forEach(entry -> observedKeys.put(entry.getKey(), entry));

            if (!entryList.isEmpty()) {
                streamEntries.put(schema, new ArrayList<>(observedKeys.values()));
            }
        });

        // This transaction data does not contain any table of interest, don't add to buffer.
        if (streamEntries.isEmpty()) {
            return Optional.empty();
        }

        CorfuStoreMetadata.Timestamp timestamp = CorfuStoreMetadata.Timestamp.newBuilder()
                .setSequence(logData.getGlobalAddress())
                .setEpoch(epoch)
                .build();

        return Optional.of(new CorfuStreamEntries(streamEntries, timestamp));
    }

    public List<DeltaStream> getStreamsList() {
        return this.streamsList;
    }

    /* This function either returns null when none of the streams have any new addresses to read
     * or returns a stream
     *  (i) when either of the streams have a trimGap(). This is because trimGap() means there is
     * data that has not been synced.
     *  (ii) when either of the streams have data. If a subset of streams have data, then the stream
     *  with the lowest address is returned
     */
    private DeltaStream findNextStreamToRead() {
        DeltaStream nextStream = null;
        long smallestAddress = Address.MAX;

        for (DeltaStream stream : streamsList) {
            if (!stream.hasNext()) {
                continue;
            }
            if (stream.trimGap()) {
                nextStream = stream;
               break;
            }
            if (nextStream == null) {
                nextStream = stream;
            }

            long nextAddress = stream.getNextAddressToRead();
            if (smallestAddress > nextAddress) {
                nextStream = stream;
                smallestAddress = nextAddress;
            }
        }

        return nextStream;
    }

    private void produce() {
        Preconditions.checkState(status.get() == StreamStatus.SYNCING);
        currentStream = findNextStreamToRead();
        // if neither of the streams have updates, move the task to runnable
        if (currentStream == null) {
            move(StreamStatus.SYNCING, StreamStatus.RUNNABLE);
            return;
        }
        Preconditions.checkState(currentStream.hasNext());
        ILogData logData = currentStream.next();
        Optional<CorfuStreamEntries> streamEntries = transform(logData);
        log.debug("producing {}@{} {} on {}", logData.getEpoch(), logData.getGlobalAddress(), logData.getType(), listenerId);

        streamEntries.ifPresent(e -> MicroMeterUtils.time(() -> listener.onNextEntry(e),
                "stream.notify.duration",
                "listener",
                listenerId));

        // Re-schedule, give other streams a chance to produce
        if (hasNext()) {
            // need to make this a safe runnable
            workerPool.execute(this);
            // We need to make sure that the task can only run on a single thread therefore we must return immediately
            return;
        }

        // No more items to produce
        move(StreamStatus.SYNCING, StreamStatus.RUNNABLE);
    }

    private boolean hasNext() {
        for (DeltaStream stream : streamsList) {
            if (stream.hasNext()) {
                return true;
            }
        }
        return false;
    }

    public void propagateError() {
        Objects.requireNonNull(error);
        if (error instanceof TrimmedException) {
            listener.onError(new StreamingException(error));
        } else {
            listener.onError(error);
        }
    }

    @Override
    public void run() {
        try {
            produce();
        } catch (Throwable throwable) {
            setError(throwable);
            if (currentStream == null) {
                log.error("StreamingTask: encountered exception {} during client notification callback, " +
                        "listener: {} name {}", throwable, listener, listenerId);
            } else {
                log.error("StreamingTask: encountered exception {} during client notification callback, " +
                        "listener: {} name {} id {}", throwable, listener, listenerId, currentStream.getStreamId());
            }
        }
    }

    public void setError(Throwable throwable) {
        status.set(StreamStatus.ERROR);
        this.error = throwable;
    }
}
