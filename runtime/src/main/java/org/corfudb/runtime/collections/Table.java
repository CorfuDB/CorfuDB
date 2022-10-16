package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import io.micrometer.core.instrument.Counter;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.CorfuGuidGenerator;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.util.serializer.ISerializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wrapper over the CorfuTable.
 * It accepts a primary key - which is a protobuf message.
 * The value is a CorfuRecord which comprises of 2 fields - Payload and Metadata. These are protobuf messages as well.
 * <p>
 */
@Slf4j
public class Table<K extends Message, V extends Message, M extends Message> {

    private CorfuTable<K, CorfuRecord<V, M>> corfuTable;

    /**
     * Namespace this table belongs in.
     */
    @Getter
    private final String namespace;

    /**
     * Fully qualified table name: created by the namespace and the table name.
     */
    @Getter
    private final String fullyQualifiedTableName;

    @Getter
    private final UUID streamUUID;

    @Getter
    private final MetadataOptions metadataOptions;

    @Getter
    private final Class<K> keyClass;

    @Getter
    private final Class<V> valueClass;

    @Getter
    private final Class<M> metadataClass;

    @Getter
    private final Set<UUID> streamTags;

    private final TableParameters<K, V, M> tableParameters;
    private final Supplier<StreamingMap<K, V>> streamingMapSupplier;
    private final ICorfuVersionPolicy.VersionPolicy versionPolicy;
    /**
     * In case this table is opened as a Queue, we need the Guid generator to support enqueue operations.
     */
    private final CorfuGuidGenerator guidGenerator;

    /**
     * Returns a Table instance backed by a CorfuTable.
     *
     * @param tableParameters      Table parameters including
     *                             table's namespace and fullyQualifiedTableName,
     *                             key, value, and metadata classes,
     *                             value and metadata schemas
     * @param corfuRuntime         connected instance of the Corfu Runtime
     * @param serializer           protobuf serializer
     * @param streamingMapSupplier supplier of underlying map data structure
     * @param versionPolicy        versioning policy
     * @param streamTags           set of UUIDs representing the streamTags
     */
    public Table(@Nonnull final TableParameters<K, V, M> tableParameters,
                 @Nonnull final CorfuRuntime corfuRuntime,
                 @Nonnull final ISerializer serializer,
                 @Nonnull final Supplier<StreamingMap<K, V>> streamingMapSupplier,
                 @NonNull final ICorfuVersionPolicy.VersionPolicy versionPolicy,
                 @NonNull final Set<UUID> streamTags) {

        this.namespace = tableParameters.getNamespace();
        this.fullyQualifiedTableName = tableParameters.getFullyQualifiedTableName();
        this.streamUUID = CorfuRuntime.getStreamID(this.fullyQualifiedTableName);
        this.streamTags = streamTags;
        this.metadataOptions = Optional.ofNullable(tableParameters.getMetadataSchema())
                .map(schema -> MetadataOptions.builder()
                        .metadataEnabled(true)
                        .defaultMetadataInstance(schema)
                        .build())
                .orElse(MetadataOptions.builder().build());
        this.tableParameters = tableParameters;
        this.streamingMapSupplier = streamingMapSupplier;
        this.versionPolicy = versionPolicy;

        this.corfuTable = corfuRuntime.getObjectsView().build()
                .setTypeToken(CorfuTable.<K, CorfuRecord<V, M>>getTableType())
                .setStreamName(this.fullyQualifiedTableName)
                .setSerializer(serializer)
                .setArguments(new ProtobufIndexer(tableParameters.getValueSchema(),
                        tableParameters.getSchemaOptions()),
                        streamingMapSupplier, versionPolicy)
                .setStreamTags(streamTags)
                .open();
        this.keyClass = tableParameters.getKClass();
        this.valueClass = tableParameters.getVClass();
        this.metadataClass = tableParameters.getMClass();
        if (keyClass == Queue.CorfuGuidMsg.class &&
                metadataClass == Queue.CorfuQueueMetadataMsg.class) { // Really a Queue
            this.guidGenerator = CorfuGuidGenerator.getInstance(corfuRuntime);
        } else {
            this.guidGenerator = null;
        }

        this.tableScanned = MicroMeterUtils.counter("scannedkvs.count");

    }
    private final Optional<Counter> tableScanned;

    /**
     * Fetch the value for a key.
     *
     * @param key Key.
     * @return Corfu Record for key.
     */
    @Nullable
    CorfuRecord<V, M> get(@Nonnull final K key) {
        return corfuTable.get(key);
    }

    /**
     * Update an existing key with the provided value. Create if it does not exist.
     *
     * @param key      Key.
     * @param value    Value.
     * @param metadata Metadata.
     */
    protected void put(@Nonnull final K key,
                          @Nonnull final V value,
                          @Nullable final M metadata) {
        corfuTable.insert(key, new CorfuRecord<>(value, metadata));
    }

    /**
     * Delete a record mapped to the specified key.
     *
     * @param key Key.
     */
    @Nullable
    protected void deleteRecord(@Nonnull final K key) {
        if (!corfuTable.containsKey(key)) {
            log.warn("Deleting a non-existent key {}", key);
            return;
        }
        corfuTable.delete(key);
    }

    /**
     * Clear All table entries.
     */
    public void clearAll() {
        corfuTable.clear();
    }

    /**
     * Allow GC to remove all the in-memory objects associated with the CorfuTable
     * and make it look like the table is a newly opened one
     * This is useful for JVMs who do not wish to keep the entire table
     * around in memory.
     * @param runtime - the runtime that was used to create this table
     * @param serializer - the serializer used to materialize table data
     */
    public void resetTableData(CorfuRuntime runtime, ISerializer serializer) {
        ObjectsView.ObjectID oid = new ObjectsView.ObjectID(getStreamUUID(), CorfuTable.class);
        Object tableObject = runtime.getObjectsView().getObjectCache().remove(oid);
        if (tableObject == null) {
            throw new NoSuchElementException("resetTableData: No object cache entry for "+ fullyQualifiedTableName);
        }
        this.corfuTable = runtime.getObjectsView().build()
                .setTypeToken(CorfuTable.<K, CorfuRecord<V, M>>getTableType())
                .setStreamName(this.fullyQualifiedTableName)
                .setSerializer(serializer)
                .setArguments(new ProtobufIndexer(tableParameters.getValueSchema(),
                                tableParameters.getSchemaOptions()),
                        streamingMapSupplier, versionPolicy)
                .setStreamTags(streamTags)
                .open();
    }

    /**
     * Appends the specified element at the end of this unbounded queue.
     * Capacity restrictions and backoffs must be implemented outside this
     * interface. Consider validating the size of the queue against a high
     * watermark before enqueue.
     *
     * @param e the element to add
     * @throws IllegalArgumentException if some property of the specified
     *                                  element prevents it from being added to this queue
     */
    public K enqueue(V e) {
        /**
         * This is a callback that is placed into the root transaction's context on
         * the thread local stack which will be invoked right after this transaction
         * is deemed successful and has obtained a final sequence number to write.
         */
        @AllArgsConstructor
        class QueueEntryAddressGetter implements TransactionalContext.PreCommitListener {
            private CorfuRecord<V, M> record;

            /**
             * If we are in a transaction, determine the commit address and fix it up in
             * the queue entry's metadata.
             * @param tokenResponse - the sequencer's token response returned.
             */
            @Override
            public void preCommitCallback(TokenResponse tokenResponse) {
                record.setMetadata((M) Queue.CorfuQueueMetadataMsg.newBuilder()
                        .setTxSequence(tokenResponse.getSequence()).build());
                log.trace("preCommitCallback for Queue: " + tokenResponse);
            }
        }

        // Obtain a cluster-wide unique 64-bit id to identify this entry in the queue.
        long entryId = guidGenerator.nextLong();
        // Embed this key into a protobuf.
        K keyOfQueueEntry = (K) Queue.CorfuGuidMsg.newBuilder().setInstanceId(entryId).build();

        // Prepare a partial record with the queue's payload and temporary metadata that will be overwritten
        // by the QueueEntryAddressGetter callback above when the transaction finally commits.
        CorfuRecord<V, M> queueEntry = new CorfuRecord<>(e,
                (M) Queue.CorfuQueueMetadataMsg.newBuilder().setTxSequence(0).build());

        QueueEntryAddressGetter addressGetter = new QueueEntryAddressGetter(queueEntry);
        log.trace("enqueue: Adding preCommitListener for Queue: " + e.toString());
        TransactionalContext.getRootContext().addPreCommitListener(addressGetter);

        corfuTable.insert(keyOfQueueEntry, queueEntry);
        return keyOfQueueEntry;
    }

    /**
     * Returns a List of CorfuQueueRecords sorted by the order in which the enqueue materialized.
     * This is the primary method of consumption of entries enqueued into CorfuQueue.
     *
     * <p>This function currently does not return a view like the java.util implementation,
     * and changes to the entryList will *not* be reflected in the map. </p>
     *
     * @return List of Entries sorted by their enqueue order
     */
    public List<CorfuQueueRecord> entryList() {
        Comparator<Map.Entry<K, CorfuRecord<V, M>>> queueComparator =
                (Map.Entry<K, CorfuRecord<V, M>> rec1, Map.Entry<K, CorfuRecord<V, M>> rec2) -> {
                    long r1EntryId = ((Queue.CorfuGuidMsg) rec1.getKey()).getInstanceId();
                    long r1Sequence = ((Queue.CorfuQueueMetadataMsg) rec1.getValue().getMetadata()).getTxSequence();

                    long r2EntryId = ((Queue.CorfuGuidMsg) rec2.getKey()).getInstanceId();
                    long r2Sequence = ((Queue.CorfuQueueMetadataMsg) rec2.getValue().getMetadata()).getTxSequence();
                    return CorfuQueueRecord.compareTo(r1EntryId, r2EntryId, r1Sequence, r2Sequence);
                };

        List<CorfuQueueRecord> copy = new ArrayList<>(corfuTable.size());
        for (Map.Entry<K, CorfuRecord<V, M>> entry : corfuTable.entryStream()
                .sorted(queueComparator).collect(Collectors.toList())) {
            copy.add(new CorfuQueueRecord((Queue.CorfuGuidMsg) entry.getKey(),
                    (Queue.CorfuQueueMetadataMsg) entry.getValue().getMetadata(),
                    entry.getValue().getPayload()));
        }
        return copy;
    }

    /**
     * CorfuQueueRecord encapsulates each entry enqueued into CorfuQueue with its unique ID.
     * It is a read-only type returned by the entryList() method.
     * The ID returned here can be used for both point get()s as well as remove() operations
     * on this Queue.
     */
    public static class CorfuQueueRecord implements Comparable<CorfuQueueRecord> {
        /**
         * This ID represents the entry and its order in the Queue.
         * This implies that it is unique and comparable with other IDs
         * returned from CorfuQueue methods with respect to its enqueue order.
         * because if this method is wrapped in a transaction, the order is established only later.
         */
        @Getter
        private final Queue.CorfuGuidMsg recordId;

        @Getter
        private final Queue.CorfuQueueMetadataMsg txSequence;

        @Getter
        private final Message entry;

        public String toString() {
            return String.format("%s | %s =>%s", recordId, txSequence, entry);
        }

        CorfuQueueRecord(Queue.CorfuGuidMsg entryId, Queue.CorfuQueueMetadataMsg txSequence, Message entry) {
            this.recordId = entryId;
            this.txSequence = txSequence;
            this.entry = entry;
        }

        public static int compareTo(long thisEntryId, long thatEntryId, long thisSequence, long thatSequence) {
            if (thisEntryId == thatEntryId && thisSequence == thatSequence) {
                return 0;
            }
            if (thisSequence > thatSequence) {
                return 1;
            }
            if (thisSequence == thatSequence && thisEntryId > thatEntryId) {
                return 1;
            }
            return -1;
        }

        @Override
        public int compareTo(CorfuQueueRecord o) {
            return compareTo(this.getRecordId().getInstanceId(), ((CorfuQueueRecord) o).recordId.getInstanceId(),
                    this.getTxSequence().getTxSequence(), ((CorfuQueueRecord) o).getTxSequence().getTxSequence());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CorfuQueueRecord that = (CorfuQueueRecord) o;
            return this.getRecordId().getInstanceId() == that.getRecordId().getInstanceId() &&
                    this.getTxSequence().getTxSequence() == that.getTxSequence().getTxSequence();
        }

        @Override
        public int hashCode() {
            return recordId.hashCode();
        }
    }

    /**
     * Count of records in the table.
     *
     * @return Count of records.
     */
    public int count() {
        return corfuTable.size();
    }

    /**
     * Keyset of the table.
     *
     * @return Returns a keyset.
     */
    Set<K> keySet() {
        return corfuTable.keySet();
    }

    /**
     * Scan and filter by entry.
     *
     * @param entryPredicate Predicate to filter the entries.
     * @return Collection of filtered entries.
     */
    @Nonnull
    List<CorfuStoreEntry<K, V, M>> scanAndFilterByEntry(
            @Nonnull final Predicate<CorfuStoreEntry<K, V, M>> entryPredicate) {
        long startTime = System.nanoTime();
        try(Stream<Map.Entry<K, CorfuRecord<V, M>>> stream = corfuTable.entryStream()) {
            List<CorfuStoreEntry<K, V, M>> res = CorfuTable.pool.submit(() -> stream
                    .filter(recordEntry ->
                            entryPredicate.test(new CorfuStoreEntry<>(
                                    recordEntry.getKey(),
                                    recordEntry.getValue().getPayload(),
                                    recordEntry.getValue().getMetadata())))
                    .parallel()
                    .map(entry -> new CorfuStoreEntry<>(
                            entry.getKey(),
                            entry.getValue().getPayload(),
                            entry.getValue().getMetadata()))
                    .collect(Collectors.toList())).join();
            MicroMeterUtils.time(Duration.ofNanos(System.nanoTime() - startTime), "table.scan",
                    "tableName", getFullyQualifiedTableName());

            MicroMeterUtils.time(Duration.ofNanos(1), "agg.scan");
            this.tableScanned.get().increment(corfuTable.size());
            return res;
        }
    }

    /**
     * Stream the whole table out in chunks, useful for very large tables
     * that won't fit completely in memory.
     *
     * @return Collection of filtered entries.
     */
    public @Nonnull
    Stream<CorfuStoreEntry<K, V, M>> entryStream() {
        return corfuTable.entryStream().map(entry ->
                new CorfuStoreEntry<>(
                        entry.getKey(),
                        entry.getValue().getPayload(),
                        entry.getValue().getMetadata()));
    }

    /**
     * Get by secondary index.
     *
     * @param <I>       Type of index key.
     * @param indexName Index name.
     * @param indexKey  Index key.
     * @return Collection of entries filtered by the secondary index.
     */
    @Nonnull
    <I>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull final String indexName,
                                              @Nonnull final I indexKey) {
        return corfuTable.getByIndex(() -> indexName, indexKey).stream()
                .map(entry -> new CorfuStoreEntry<K, V, M>(entry.getKey(),
                        entry.getValue().getPayload(),
                        entry.getValue().getMetadata()))
                .collect(Collectors.toList());
    }
}
