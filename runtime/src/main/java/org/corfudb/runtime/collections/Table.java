package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CheckpointWriter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.Queue;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.PersistenceOptions.PersistenceOptionsBuilder;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.CorfuGuidGenerator;
import org.corfudb.runtime.view.ObjectsView.ObjectID;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.SafeProtobufSerializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wrapper over the CorfuTable.
 * It accepts a primary key - which is a protobuf message.
 * The value is a CorfuRecord which comprises of 2 fields - Payload and Metadata. These are protobuf messages as well.
 * <p>
 */
@Slf4j
public class Table<K extends Message, V extends Message, M extends Message> implements AutoCloseable {

    // Accessor/Mutator threads can interleave in a way that create a deadlock because they can create a
    // circular dependency between the VersionLockedObject(VLO) lock and the common forkjoin thread pool. In order
    // to break the dependency, parallel stream operations have to execute on a separate pool that applications
    // cant use. For example, if there are 4 accessor threads all using the common forkjoin pool, one of the threads
    // can acquire the VLO lock and cause the other 3 threads to wait, but after acquiring the VLO lock, the thread
    // gets block on parallel stream, because the pool is exhausted with threads that are trying to acquire the VLO
    // look, which creates a circular dependency. In other words, a deadlock.

    protected static final ForkJoinPool pool = new ForkJoinPool(Math.max(Runtime.getRuntime().availableProcessors() / 2, 1),
            pool -> {
                final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                worker.setName("Table-Forkjoin-pool-" + worker.getPoolIndex());
                return worker;
            }, null, true);

    public ICorfuTable<K, CorfuRecord<V, M>> corfuTable;

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
    /**
     * In case this table is opened as a Queue, we need the Guid generator to support enqueue operations.
     */
    private final CorfuGuidGenerator guidGenerator;

    @Getter
    private final ISerializer serializer;

    /**
     * Returns a Table instance backed by a CorfuTable.
     *
     * @param tableParameters      Table parameters including
     *                             table's namespace and fullyQualifiedTableName,
     *                             key, value, and metadata classes,
     *                             value and metadata schemas
     * @param corfuRuntime         connected instance of the Corfu Runtime
     * @param serializer           protobuf serializer
     * @param streamTags           set of UUIDs representing the streamTags
     */
    public Table(@Nonnull final TableParameters<K, V, M> tableParameters,
                 @Nonnull final CorfuRuntime corfuRuntime,
                 @Nonnull final ISerializer serializer,
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
        this.serializer = serializer;

        initializeCorfuTable(corfuRuntime);

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
     * DO NOT USE THIS METHOD WITH NO_CACHE TABLES
     * @param runtime - the runtime that was used to create this table
     */
    public void resetTableData(CorfuRuntime runtime) {
        if (!corfuTable.isTableCached()) {
            throw new IllegalStateException("Cannot reset a table opened with NO_CACHE option.");
        }


        // PersistentCorfuTable
        ObjectID<?> oid = ObjectID.builder().streamID(getStreamUUID()).build();

        // Evict all versions of this Table from MVOCache
        runtime.getObjectsView().getMvoCache().invalidateAllVersionsOf(getStreamUUID());
        corfuTable.close();

        Object tableObject = runtime.getObjectsView().getObjectCache().remove(oid);
        if (tableObject == null) {
            throw new NoSuchElementException("resetTableData: No object cache entry for "+ fullyQualifiedTableName);
        }

        initializeCorfuTable(runtime);
    }

    private void initializeCorfuTable(CorfuRuntime runtime) {
        final SMRObject.Builder<? extends ICorfuTable<K, CorfuRecord<V, M>>> builder;
        final Deque<Object> arguments = new ArrayDeque<>();

        // Check to see if we should be used a disk backed table.
        if (tableParameters.getPersistenceOptions().hasDataPath()) {
            PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder();
            persistenceOptions.dataPath(Paths.get(tableParameters.getPersistenceOptions().getDataPath()));
            if (tableParameters.getPersistenceOptions().hasConsistencyModel()) {
                persistenceOptions.consistencyModel(tableParameters.getPersistenceOptions().getConsistencyModel());
            }
            if (tableParameters.getPersistenceOptions().hasSizeComputationModel()) {
                persistenceOptions.sizeComputationModel(tableParameters.getPersistenceOptions().getSizeComputationModel());
            }
            if (tableParameters.getPersistenceOptions().hasWriteBufferSize()) {
                persistenceOptions.writeBufferSize(Optional.of(tableParameters.getPersistenceOptions().getWriteBufferSize()));
            }

            ISerializer safeSerializer = new SafeProtobufSerializer(serializer);
            builder = runtime.getObjectsView().<PersistedCorfuTable<K, CorfuRecord<V, M>>>build()
                    .setStreamName(fullyQualifiedTableName)
                    .setStreamTags(streamTags)
                    .setSerializer(safeSerializer)
                    .setTypeToken(PersistedCorfuTable.getTypeToken());

            arguments.add(persistenceOptions.build());
            arguments.add(DiskBackedCorfuTable.defaultOptions);
            arguments.add(safeSerializer);
        } else {
            // Default in-memory implementation.
            builder = runtime.getObjectsView().<PersistentCorfuTable<K, CorfuRecord<V, M>>>build()
                    .setStreamName(fullyQualifiedTableName)
                    .setStreamTags(streamTags)
                    .setSerializer(serializer)
                    .setTypeToken(PersistentCorfuTable.getTypeToken());
        }

        if (!tableParameters.isSecondaryIndexesDisabled()) {
            arguments.add(new ProtobufIndexer(
                    tableParameters.getValueSchema(),
                    tableParameters.getSchemaOptions()));
        }

        builder.setArguments(arguments.toArray());
        this.corfuTable = builder.open();
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
        Comparator<Entry<K, CorfuRecord<V, M>>> queueComparator =
                (Entry<K, CorfuRecord<V, M>> rec1, Entry<K, CorfuRecord<V, M>> rec2) -> {
                    long r1EntryId = ((Queue.CorfuGuidMsg) rec1.getKey()).getInstanceId();
                    long r1Sequence = ((Queue.CorfuQueueMetadataMsg) rec1.getValue().getMetadata()).getTxSequence();

                    long r2EntryId = ((Queue.CorfuGuidMsg) rec2.getKey()).getInstanceId();
                    long r2Sequence = ((Queue.CorfuQueueMetadataMsg) rec2.getValue().getMetadata()).getTxSequence();
                    return CorfuQueueRecord.compareTo(r1EntryId, r2EntryId, r1Sequence, r2Sequence);
                };

        List<CorfuQueueRecord> copy = new ArrayList<>(corfuTable.size());
        for (Entry<K, CorfuRecord<V, M>> entry : corfuTable.entryStream()
                .sorted(queueComparator).collect(Collectors.toList())) {
            copy.add(new CorfuQueueRecord((Queue.CorfuGuidMsg) entry.getKey(),
                    (Queue.CorfuQueueMetadataMsg) entry.getValue().getMetadata(),
                    entry.getValue().getPayload()));
        }
        return copy;
    }

    @Override
    public void close() throws Exception {
        corfuTable.close();
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
        try(Stream<Entry<K, CorfuRecord<V, M>>> stream = corfuTable.entryStream()) {
            List<CorfuStoreEntry<K, V, M>> res = pool.submit(() -> stream
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
        return StreamSupport.stream(corfuTable.getByIndex(() -> indexName, indexKey).spliterator(), false)
                .map(entry -> new CorfuStoreEntry<>(entry.getKey(),
                        entry.getValue().getPayload(),
                        entry.getValue().getMetadata()))
                .collect(Collectors.toList());
    }

    public CheckpointWriter<ICorfuTable<?,?>> getCheckpointWriter(CorfuRuntime rt, String author) {
        CheckpointWriter<ICorfuTable<?,?>> cpw = new CheckpointWriter(rt, streamUUID, author, this.corfuTable);
        cpw.setSerializer(serializer);
        return cpw;
    }

    public Class<?> getUnderlyingType() {
        return corfuTable.getClass();
    }
}
