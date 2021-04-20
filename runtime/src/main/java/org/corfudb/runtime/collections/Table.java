package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.CorfuGuidGenerator;
import org.corfudb.util.serializer.ISerializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    private final CorfuRuntime corfuRuntime;

    private final CorfuTable<K, CorfuRecord<V, M>> corfuTable;

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

    /**
     * List of Metrics captured on this table
     */
    @Getter
    private final TableMetrics metrics;

    @Getter
    private final Class<K> keyClass;

    @Getter
    private final Class<V> valueClass;

    @Getter
    private final Class<M> metadataClass;

    @Getter
    private final Set<UUID> streamTags;

    /**
     * In case this table is opened as a Queue, we need the Guid generator to support enqueue operations.
     */
    private final CorfuGuidGenerator guidGenerator;

    /**
     * Returns a Table instance backed by a CorfuTable.
     *
     * @param tableParameters         Table parameters including
     *                                table's namespace and fullyQualifiedTableName,
     *                                key, value, and metadata classes,
     *                                value and metadata schemas
     * @param corfuRuntime            connected instance of the Corfu Runtime
     * @param serializer              protobuf serializer
     * @param streamingMapSupplier    supplier of underlying map data structure
     * @param versionPolicy           versioning policy
     * @param streamTags              set of UUIDs representing the streamTags
     */
    @Nonnull
    public Table(@Nonnull final TableParameters<K, V, M> tableParameters,
                 @Nonnull final CorfuRuntime corfuRuntime,
                 @Nonnull final ISerializer serializer,
                 @Nonnull final Supplier<StreamingMap<K, V>> streamingMapSupplier,
                 @NonNull final ICorfuVersionPolicy.VersionPolicy versionPolicy,
                 @NonNull final Set<UUID> streamTags) {

        this.corfuRuntime = corfuRuntime;
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

        this.corfuTable = corfuRuntime.getObjectsView().build()
                .setTypeToken(CorfuTable.<K, CorfuRecord<V, M>>getTableType())
                .setStreamName(this.fullyQualifiedTableName)
                .setSerializer(serializer)
                .setArguments(new ProtobufIndexer(tableParameters.getValueSchema()), streamingMapSupplier, versionPolicy)
                .setStreamTags(streamTags)
                .open();
        this.metrics = new TableMetrics(this.fullyQualifiedTableName);
        this.keyClass = tableParameters.getKClass();
        this.valueClass = tableParameters.getVClass();
        this.metadataClass = tableParameters.getMClass();
        if (keyClass == Queue.CorfuGuidMsg.class &&
                metadataClass == Queue.CorfuQueueMetadataMsg.class) { // Really a Queue
            this.guidGenerator = CorfuGuidGenerator.getInstance(corfuRuntime);
        } else {
            this.guidGenerator = null;
        }
    }

    /**
     * Begins an optimistic transaction under the assumption that no transactional reads would be done.
     */
    private boolean TxBegin() {
        if (TransactionalContext.isInTransaction()) {
            return false;
        }
        corfuRuntime.getObjectsView()
                .TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE)
                .build()
                .begin();
        return true;
    }

    /**
     * Ends an ongoing transaction.
     */
    private void TxEnd() {
        corfuRuntime.getObjectsView().TXEnd();
    }

    /**
     * Create a new record.
     *
     * @param key      Key
     * @param value    Value
     * @param metadata Record metadata.
     * @return Previously stored record if any.
     */
    @Nullable
    @Deprecated
    CorfuRecord<V, M> create(@Nonnull final K key,
                             @Nullable final V value,
                             @Nullable final M metadata) {
        boolean beganNewTxn = false;
        try {
            beganNewTxn = TxBegin();
            CorfuRecord<V, M> previous = corfuTable.get(key);
            if (previous != null) {
                throw new RuntimeException("Cannot create a record on existing key.");
            }
            M newMetadata = null;
            if (metadataOptions.isMetadataEnabled()) {
                if (metadata == null) {
                    throw new RuntimeException("Table::create needs non-null metadata");
                }
                M metadataDefaultInstance = (M) getMetadataOptions().getDefaultMetadataInstance();
                newMetadata = mergeOldAndNewMetadata(metadataDefaultInstance, metadata, true);
            }
            return corfuTable.put(key, new CorfuRecord<>(value, newMetadata));
        } finally {
            if (beganNewTxn) {
                TxEnd();
            }
        }
    }

    /**
     * Fetch the value for a key.
     *
     * @param key Key.
     * @return Corfu Record for key.
     */
    @Nullable
    public CorfuRecord<V, M> get(@Nonnull final K key) {
        return corfuTable.get(key);
    }

    /**
     * Update an existing key with the provided value.
     *
     * @param key      Key.
     * @param value    Value.
     * @param metadata Metadata.
     * @return Previously stored value for the provided key.
     */
    @Nullable
    @Deprecated
    CorfuRecord<V, M> update(@Nonnull final K key,
                             @Nonnull final V value,
                             @Nullable final M metadata) {
        boolean beganNewTxn = false;
        try {
            beganNewTxn = TxBegin();
            M newMetadata = null;
            if (metadataOptions.isMetadataEnabled()) {
                if (metadata == null) {
                    throw new RuntimeException("Table::update needs non-null metadata");
                }
                CorfuRecord<V, M> previous = corfuTable.get(key);
                M previousMetadata;
                boolean isCreate = false;
                if (previous == null) { // Really a create() call not an update.
                    previousMetadata = (M) metadataOptions.getDefaultMetadataInstance();
                    isCreate = true;
                } else {
                    previousMetadata = previous.getMetadata();
                }
                validateVersion(previousMetadata, metadata);
                newMetadata = mergeOldAndNewMetadata(previousMetadata, metadata, isCreate);
            }
            return corfuTable.put(key, new CorfuRecord<>(value, newMetadata));
        } finally {
            if (beganNewTxn) {
                TxEnd();
            }
        }
    }

    /**
     * Update an existing key with the provided value. Create if it does not exist.
     *
     * @param key      Key.
     * @param value    Value.
     * @param metadata Metadata.
     * @return Previously stored value for the provided key - null if create.
     */
    CorfuRecord<V, M> put(@Nonnull final K key,
                          @Nonnull final V value,
                          @Nullable final M metadata) {
        return corfuTable.put(key, new CorfuRecord<>(value, metadata));
    }

    /**
     * Delete a record mapped to the specified key.
     *
     * @param key Key.
     * @return Previously stored Corfu Record.
     */
    @Nullable
    @Deprecated
    CorfuRecord<V, M> delete(@Nonnull final K key) {
        boolean beganNewTxn = false;
        try {
            beganNewTxn = TxBegin();
            return corfuTable.remove(key);
        } finally {
            if (beganNewTxn) {
                TxEnd();
            }
        }
    }

    /**
     * Delete a record mapped to the specified key.
     *
     * @param key Key.
     * @return Previously stored Corfu Record.
     */
    @Nullable
    CorfuRecord<V, M> deleteRecord(@Nonnull final K key) {
        return corfuTable.remove(key);
    }

    /**
     * Clear All table entries.
     */
    void clearAll() {
       corfuTable.clear();
    }

    /**
     * Clears the table.
     */
    @Deprecated
    public void clear() {
        boolean beganNewTxn = false;
        try {
            beganNewTxn = TxBegin();
            clearAll();
        } finally {
            if (beganNewTxn) {
                TxEnd();
            }
        }
    }

    /**
     * Appends the specified element at the end of this unbounded queue.
     * Capacity restrictions and backoffs must be implemented outside this
     * interface. Consider validating the size of the queue against a high
     * watermark before enqueue.
     *
     * @param e the element to add
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
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
                record.setMetadata((M)Queue.CorfuQueueMetadataMsg.newBuilder()
                        .setTxSequence(tokenResponse.getSequence()).build());
                log.trace("preCommitCallback for Queue: " + tokenResponse);
            }
        }

        // Obtain a cluster-wide unique 64-bit id to identify this entry in the queue.
        long entryId = guidGenerator.nextLong();
        // Embed this key into a protobuf.
        K keyOfQueueEntry = (K)Queue.CorfuGuidMsg.newBuilder().setInstanceId(entryId).build();

        // Prepare a partial record with the queue's payload and temporary metadata that will be overwritten
        // by the QueueEntryAddressGetter callback above when the transaction finally commits.
        CorfuRecord<V, M> queueEntry = new CorfuRecord<>(e,
                (M)Queue.CorfuQueueMetadataMsg.newBuilder().setTxSequence(0).build());

        QueueEntryAddressGetter addressGetter = new QueueEntryAddressGetter(queueEntry);
        log.trace("enqueue: Adding preCommitListener for Queue: " + e.toString());
        TransactionalContext.getRootContext().addPreCommitListener(addressGetter);

        corfuTable.put(keyOfQueueEntry, queueEntry);
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
    public List<CorfuQueueRecord>  entryList() {
        Comparator<Map.Entry<K, CorfuRecord<V,M>>> queueComparator =
                (Map.Entry<K, CorfuRecord<V,M>> rec1, Map.Entry<K, CorfuRecord<V,M>> rec2) -> {
                    long r1EntryId = ((Queue.CorfuGuidMsg)rec1.getKey()).getInstanceId();
                    long r1Sequence = ((Queue.CorfuQueueMetadataMsg)rec1.getValue().getMetadata()).getTxSequence();

                    long r2EntryId = ((Queue.CorfuGuidMsg)rec2.getKey()).getInstanceId();
                    long r2Sequence = ((Queue.CorfuQueueMetadataMsg)rec2.getValue().getMetadata()).getTxSequence();
                    return CorfuQueueRecord.compareTo(r1EntryId, r2EntryId, r1Sequence, r2Sequence);
                };

        List<CorfuQueueRecord> copy = new ArrayList<>(corfuTable.size());
        for (Map.Entry<K, CorfuRecord<V,M>> entry : corfuTable.entryStream()
                .sorted(queueComparator).collect(Collectors.toList())) {
            copy.add(new CorfuQueueRecord((Queue.CorfuGuidMsg)entry.getKey(),
                    (Queue.CorfuQueueMetadataMsg)entry.getValue().getMetadata(),
                    entry.getValue().getPayload()));
        }
        return copy;
    }

    /**
     * CorfuQueueRecord encapsulates each entry enqueued into CorfuQueue with its unique ID.
     * It is a read-only type returned by the entryList() method.
     * The ID returned here can be used for both point get()s as well as remove() operations
     * on this Queue.
     *
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
     * Scan and filter.
     *
     * @param p Predicate to filter the values.
     * @return Collection of filtered values.
     */
    @Nonnull
    Collection<CorfuRecord<V, M>> scanAndFilter(@Nonnull final Predicate<CorfuRecord<V, M>> p) {
        return corfuTable.scanAndFilter(p);
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
        return corfuTable.scanAndFilterByEntry(recordEntry ->
                entryPredicate.test(new CorfuStoreEntry<>(
                        recordEntry.getKey(),
                        recordEntry.getValue().getPayload(),
                        recordEntry.getValue().getMetadata())))
                .parallelStream()
                .map(entry -> new CorfuStoreEntry<>(
                        entry.getKey(),
                        entry.getValue().getPayload(),
                        entry.getValue().getMetadata()))
                .collect(Collectors.toList());
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

    /**
     * Get by secondary index.
     *
     * @param <I>       Type of index key.
     * @param indexName Index name.
     * @param indexKey  Index key.
     * @return Collection of entries filtered by the secondary index.
     */
    @Nonnull
    protected <I>
    Collection<Map.Entry<K, V>> getByIndexAsQueryResult(@Nonnull final String indexName,
                                                        @Nonnull final I indexKey) {
        return corfuTable.getByIndex(() -> indexName, indexKey).stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().getPayload()))
                .collect(Collectors.toList());
    }

    private Set<Descriptors.FieldDescriptor.Type> versionTypes = new HashSet<>(Arrays.asList(
            Descriptors.FieldDescriptor.Type.INT32,
            Descriptors.FieldDescriptor.Type.INT64,
            Descriptors.FieldDescriptor.Type.UINT32,
            Descriptors.FieldDescriptor.Type.UINT64,
            Descriptors.FieldDescriptor.Type.SFIXED32,
            Descriptors.FieldDescriptor.Type.SFIXED64
    ));

    @Deprecated
    private M mergeOldAndNewMetadata(@Nonnull M previousMetadata,
                                     @Nullable M userMetadata, boolean isCreate) {
        M.Builder builder = previousMetadata.toBuilder();
        for (Descriptors.FieldDescriptor fieldDescriptor : previousMetadata.getDescriptorForType().getFields()) {
            if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getVersion()) {
                // If an object is just being created, explicitly set its version field to 0
                builder.setField(
                        fieldDescriptor,
                        Optional.ofNullable(previousMetadata.getField(fieldDescriptor))
                                .map(previousVersion -> (isCreate ? 0L : ((Long) previousVersion) + 1))
                                .orElse(0L));
            } else if (userMetadata != null) { // Non-revision fields must retain previous values..
                if (!userMetadata.hasField(fieldDescriptor)) { // ..iff not explicitly set..
                    builder.setField(fieldDescriptor, previousMetadata.getField(fieldDescriptor));
                } else { // .. otherwise the values of newer fields that are explicitly set are taken.
                    builder.setField(fieldDescriptor, userMetadata.getField(fieldDescriptor));
                }
            }
        }
        return (M) builder.build();
    }

    @Deprecated
    private void validateVersion(@Nullable M previousMetadata,
                                 @Nullable M userMetadata) {
        // TODO: do a lookup instead of a search if possible
        for (Descriptors.FieldDescriptor fieldDescriptor : userMetadata.getDescriptorForType().getFields()) {
            if (!fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getVersion()) {
                continue;
            }
            if (!versionTypes.contains(fieldDescriptor.getType())) {
                throw new IllegalArgumentException("Version field needs to be an Integer or Long type."
                        + " Current type=" + fieldDescriptor.getType());
            }
            long validatingVersion = (long) userMetadata.getField(fieldDescriptor);
            if (validatingVersion <= 0) {
                continue; // Don't validate if user has not explicitly set the version field.
            }
            long previousVersion = Optional.ofNullable(previousMetadata)
                    .map(m -> (Long) m.getField(fieldDescriptor))
                    .orElse(-1L);
            if (validatingVersion != previousVersion) {
                throw new RuntimeException("Stale object Exception: " + previousVersion + " != "
                        + validatingVersion);
            }
        }
    }
}
