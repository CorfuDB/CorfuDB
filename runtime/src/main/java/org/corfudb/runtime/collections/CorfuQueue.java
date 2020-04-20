package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.format.Types;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext.PreCommitListener;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.CorfuGuidGenerator;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Persisted Queue supported by CorfuDB using distributed State Machine Replication.
 * Entries enqueued are backed by a CorfuTable with a LinkedHashMap where each entry
 * is mapped to a unique generated id that is returned upon successful <b>enqueue()</b>.
 * <b>entryList()</b> returns the enqueued entries along with their special ids which can
 * not only identify the element for removal but also has a global ordering defined by
 * the order in which their <b>enqueue()</b> operations materialized.
 * Instead of a dequeue() this Queue supports a <b>remove()</b> which accepts the id of the element.
 * Entries cannot be modified in-place (or will lose ordering) but can be removed from anywhere
 * from the persisted queue.
 *
 * Created by hisundar on 5/8/19.
 *
 * @param <E>   Type of the entry to be enqueued into the persisted queue
 */
@Slf4j
public class CorfuQueue<E> {
    /**
     * The main CorfuTable which contains the primary key-value mappings.
     */
    private final CorfuTable<Long, CorfuQueueRecord<ByteString>> corfuTable;
    private final CorfuGuidGenerator guidGenerator;

    public CorfuQueue(CorfuRuntime runtime, String streamName, ISerializer serializer,
                      Index.Registry<Long, E> indices) {
        final Supplier<StreamingMap<Long, E>> mapSupplier =
                () -> new StreamingMapDecorator<>(new LinkedHashMap<Long, E>());
        corfuTable = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Long, CorfuQueueRecord<ByteString>>>() {})
                .setStreamName(streamName)
                .setArguments(indices, mapSupplier)
                .setSerializer(serializer)
                .open();
        guidGenerator = CorfuGuidGenerator.getInstance(runtime);
    }

    public CorfuQueue(CorfuRuntime runtime, String streamName) {
        this(runtime, streamName, Serializers.getDefaultSerializer(), Index.Registry.empty());

    }

    /**
     * Returns the size of the queue at a given point in time.
     */
    public int size() {
        return corfuTable.size();
    }

    /**
     * Each entry in the Queue is tagged with a unique Id. Internally this Id is a long.
     * However, once we get all the entries out via entryList() api, these Ids are prefixed
     * with their transactional sequence numbers which represents order if enqueue()
     * were in wrapped a corfu transaction.
     * This class encapsulates these two longs into one Id and add rules on comparability.
     */
    @EqualsAndHashCode
    public static class CorfuRecordId implements Comparable<CorfuRecordId> {
        @Setter
        @Getter
        private long txSequence;

        @Getter
        private long entryId;

        public CorfuRecordId(long txSequence, long entryId) {
            this.txSequence = txSequence;
            this.entryId = entryId;
        }

        /**
         * @param from - deserialize from something returned by toByteArray()
         * @throws InvalidProtocolBufferException - invalid set of bytes
         */
        public CorfuRecordId(byte[] from) throws InvalidProtocolBufferException {
            Types.CorfuQueueIdMsg to = Types.CorfuQueueIdMsg.parseFrom(from);
            this.txSequence = to.getTxSequence();
            this.entryId = to.getEntryId();
        }

        /**
         * @return serialized representation of the CorfuRecordId as a byte[]
         */
        public byte[] toByteArray() {
            return Types.CorfuQueueIdMsg.newBuilder()
                    .setTxSequence(txSequence)
                    .setEntryId(entryId)
                    .build().toByteArray();
        }

        /**
         * It's NOT ok to compare two objects if their txSequence metadata is dissimilar.
         * @param o object to compare against.
         * @return results of comparison.
         */
        @Override
        public int compareTo(CorfuRecordId o) {
            return Comparator.comparing(CorfuRecordId::getTxSequence)
                    .thenComparing(CorfuRecordId::getEntryId)
                    .compare(this, o);
        }

        public String toString() {
            return String.format("%s|%s", txSequence, entryId);
        }
    }

    /**
     * Appends the specified element at the end of this unbounded queue.
     * In a distributed system, the linearizable order of insertions cannot be guaranteed
     * unless a transaction is used.
     * Capacity restrictions and backoffs must be implemented outside this
     * interface. Consider validating the size of the queue against a high
     * watermark before enqueue.
     * WARNING: CorfuQueue won't work if transactional enqueues are mixed with
     *          non-transactional enqueues.
     *          Either use transactions for all enqueues or none.
     *
     * @param e the element to add
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     */
    public CorfuRecordId enqueue(ByteString e) {
        final Long id = guidGenerator.nextLong();
        CorfuQueueRecord<ByteString> queueEntry;

        // If we are in a transaction, then we need the commit address of this transaction
        // to fix up as the txSequence
        if (TransactionalContext.isInTransaction()) {
            queueEntry = new CorfuQueueRecord<>(Address.NON_ADDRESS, id, e);
            /**
             * This is a callback that is placed into the root transaction's context on
             * the thread local stack which will be invoked right after this transaction
             * is deemed successful and has obtained a final sequence number to write.
             */
            class QueueEntryAddressGetter implements PreCommitListener {
                private CorfuQueueRecord<ByteString> queueRecord;
                private QueueEntryAddressGetter(CorfuQueueRecord<ByteString> queueRecord) {
                    this.queueRecord = queueRecord;
                }

                /**
                 * If we are in a transaction, determine the commit address and fix it up in
                 * the queue entry.
                 * @param tokenResponse
                 */
                @Override
                public void preCommitCallback(TokenResponse tokenResponse) {
                    queueRecord.getRecordId().setTxSequence(tokenResponse.getSequence());
                    log.trace("preCommitCallback for Queue: " + queueRecord.getRecordId().toString());
                }
            }
            QueueEntryAddressGetter addressGetter = new QueueEntryAddressGetter(queueEntry);
            log.trace("enqueue: Adding preCommitListener for Queue: " + queueEntry.getRecordId().toString());
            TransactionalContext.getRootContext().addPreCommitListener(addressGetter);
        } else {
            queueEntry = new CorfuQueueRecord<>(0, id, e);
        }

        corfuTable.put(queueEntry.getRecordId().getEntryId(), queueEntry);
        return queueEntry.getRecordId();
    }

    /**
     * CorfuQueueRecord encapsulates each entry enqueued into CorfuQueue with its unique ID.
     * It is a read-only type returned by the entryList() method.
     * The ID returned here can be used for both point get()s as well as remove() operations
     * on this Queue.
     *
     * @param <E>
     */
    public static class CorfuQueueRecord<E> implements Comparable<CorfuQueueRecord<? extends E>> {
        /**
         * This ID represents the entry and its order in the Queue.
         * This implies that it is unique and comparable with other IDs
         * returned from CorfuQueue methods with respect to its enqueue order.
         * because if this method is wrapped in a transaction, the order is established only later.
         */
        @Getter
        @Setter
        private final CorfuRecordId recordId;

        @Getter
        private final ByteString entry;

        public String toString() {
            return String.format("%s=>%s", recordId, entry);
        }

        CorfuQueueRecord(long txSequence, long entryId, ByteString entry) {
            this.recordId = new CorfuRecordId(txSequence, entryId);
            this.entry = entry;
        }

        @Override
        public int compareTo(CorfuQueueRecord<? extends E> o) {
            return this.recordId.compareTo(o.getRecordId());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CorfuQueueRecord<?> that = (CorfuQueueRecord<?>) o;
            return getRecordId().equals(that.getRecordId());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getRecordId());
        }
    }

    /**
     * Returns a List of CorfuQueueRecords sorted by the order in which the enqueue materialized.
     * This is the primary method of consumption of entries enqueued into CorfuQueue.
     *
     * <p>This function currently does not return a view like the java.util implementation,
     * and changes to the entryList will *not* be reflected in the map. </p>
     *
     * @param entriesAfter - Return only entries greater than this entry in the Queue.
     * @param maxEntries - Limit the number of entries returned from start of the queue
     * @throws IllegalArgumentException if maxEntries is negative.
     * @return List of Entries sorted by their enqueue order
     */
    public List<CorfuQueueRecord<ByteString>> entryList(CorfuRecordId entriesAfter, int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("entryList can't take zero or negative maxEntries");
        }
        log.trace("entryList: "+maxEntries+" entries after:"+entriesAfter);

        List<CorfuQueueRecord<ByteString>> copy = new ArrayList<>(
                Math.min(corfuTable.size(), maxEntries)
        );

        Comparator<Map.Entry<Long, CorfuQueueRecord<ByteString>>> recordIdComparator = (r1, r2) ->
                r1.getValue().getRecordId().compareTo(r2.getValue().recordId);
        for (Map.Entry<Long, CorfuQueueRecord<ByteString>> entry : corfuTable.entryStream()
                .filter(e -> e.getValue().getRecordId().compareTo(entriesAfter) > 0)
                .limit(maxEntries)
                .sorted(recordIdComparator).collect(Collectors.toList())) {
            copy.add(entry.getValue());
        }
        return copy;
    }

    /**
     * @return all the entries in the Queue
     */
    public List<CorfuQueueRecord<ByteString>> entryList() {
        return this.entryList(new CorfuRecordId(0,0), Integer.MAX_VALUE);
    }

    /**
     * @param maxEntries limit number of entries returned to this.
     * @return all the entries in the Queue
     */
    public List<CorfuQueueRecord<ByteString>> entryList(int maxEntries) {
        return this.entryList(new CorfuRecordId(0, 0), maxEntries);
    }

    public boolean isEmpty() {
        return corfuTable.isEmpty();
    }

    public boolean containsKey(CorfuRecordId key) {
        return corfuTable.containsKey(key.getEntryId());
    }

    /**
     * Directly retrieve the enqueued element from the CorfuTable
     * @param key
     * @return
     */
    public ByteString get(CorfuRecordId key) {
        return corfuTable.get(key.getEntryId()).getEntry();
    }

    /**
     * Removes a specific element identified by the ID returned via enqueue() or entryList()'s CorfuRecord.
     *
     * @return The entry that was successfully removed or null if there was no mapping.
     */
    public ByteString removeEntry(CorfuRecordId entryId) {
        return corfuTable.remove(entryId.getEntryId()).getEntry();
    }

    /**
     * Remove all entries from the Queue.
     */
    public void clear() {
        corfuTable.clear();
    }

    public int hashCode() {
        return Objects.hash(this);
    }

    public boolean equals(Object o){
        return o.hashCode() == Objects.hash(this);
    }

    public String toString(){
        StringBuilder stringBuilder = new StringBuilder(corfuTable.size());
        stringBuilder.append("{");
        for (Map.Entry<Long, CorfuQueueRecord<ByteString>> entry : corfuTable.entrySet()) {
            stringBuilder.append(entry.toString()).append(", ");
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }
}
