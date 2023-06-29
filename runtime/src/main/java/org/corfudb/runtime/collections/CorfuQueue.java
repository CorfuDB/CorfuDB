package org.corfudb.runtime.collections;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.Queue.CorfuQueueIdMsg;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext.PreCommitListener;
import org.corfudb.runtime.view.CorfuGuidGenerator;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.serializer.ICorfuHashable;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

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
 */
@Slf4j
public class CorfuQueue {
    /**
     * The main CorfuTable which contains the primary key-value mappings.
     */
    private final PersistentCorfuTable<CorfuRecordId, ByteString> corfuTable;
    private final CorfuGuidGenerator guidGenerator;

    @VisibleForTesting
    CorfuQueue(CorfuRuntime runtime, String streamName, ISerializer serializer) {
        corfuTable = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<CorfuRecordId, ByteString>>() {})
                .setStreamName(streamName)
                .setSerializer(serializer)
                .open();

        // TODO(Zach): Probably broken - Fix setArguments

        guidGenerator = CorfuGuidGenerator.getInstance(runtime);
    }

    public CorfuQueue(CorfuRuntime runtime, String streamName) {
        this(runtime, streamName, Serializers.QUEUE_SERIALIZER);
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
    public static class CorfuRecordId implements Comparable<CorfuRecordId>, ICorfuHashable {
        @Setter
        @Getter
        private long txSequence;

        @Getter
        private long entryId;

        public CorfuRecordId(long txSequence, long entryId) {
            this.txSequence = txSequence;
            this.entryId = entryId;
        }

        public void serialize(ByteBuf buf) {
            buf.writeLong(txSequence);
            buf.writeLong(entryId);
        }

        public static CorfuRecordId deserialize(ByteBuf buf) {
            long txSequence = buf.readLong();
            long entryId = buf.readLong();
            return new CorfuRecordId(txSequence, entryId);
        }

        /**
         * @param from - deserialize from something returned by toByteArray()
         * @throws InvalidProtocolBufferException - invalid set of bytes
         */
        public CorfuRecordId(byte[] from) throws InvalidProtocolBufferException {
            CorfuQueueIdMsg to = CorfuQueueIdMsg.parseFrom(from);
            this.txSequence = to.getTxSequence();
            this.entryId = to.getEntryId();
        }

        /**
         * @param from - the CorfuQueueIdMsg protobuf to convert from
         */
        public CorfuRecordId(CorfuQueueIdMsg from) {
            this.txSequence = from.getTxSequence();
            this.entryId = from.getEntryId();
        }

        /**
         * @return - the protobuf representation of CorfuRecordId
         */
        public CorfuQueueIdMsg toCorfuQueueIdMsg() {
            return CorfuQueueIdMsg.newBuilder()
                    .setTxSequence(this.txSequence)
                    .setEntryId(this.entryId)
                    .build();
        }

        /**
         * @return serialized representation of the CorfuRecordId as a byte[]
         */
        public byte[] toByteArray() {
            return CorfuQueueIdMsg.newBuilder()
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CorfuRecordId that = (CorfuRecordId) o;
            return getEntryId() == that.getEntryId();
        }

        @Override
        public byte[] generateCorfuHash() {
            // Note that this hash is used for transaction conflict resolution and shouldn't
            // generate collisions for different entries
            long entryId = getEntryId();
            return new byte[] {
                    (byte) (entryId >> 56),
                    (byte) (entryId >> 48),
                    (byte) (entryId >> 40),
                    (byte) (entryId >> 32),
                    (byte) (entryId >> 24),
                    (byte) (entryId >> 16),
                    (byte) (entryId >> 8),
                    (byte) entryId
            };
        }

        @Override
        public int hashCode() {
            return Objects.hash(getEntryId());
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
        checkState(TransactionalContext.isInTransaction(), "must be called within a transaction!");

        final CorfuRecordId id = new CorfuRecordId(0, guidGenerator.nextLong());

        /**
         * This is a callback that is placed into the root transaction's context on
         * the thread local stack which will be invoked right after this transaction
         * is deemed successful and has obtained a final sequence number to write.
         */
        @AllArgsConstructor
         class QueueEntryAddressGetter implements PreCommitListener {
             private final CorfuRecordId recordId;

             /**
              * If we are in a transaction, determine the commit address and fix it up in
              * the queue entry.
              * @param tokenResponse
              */
             @Override
             public void preCommitCallback(TokenResponse tokenResponse) {
                 recordId.setTxSequence(tokenResponse.getSequence());
                 log.trace("preCommitCallback for Queue: " + recordId.toString());
             }
         }

         QueueEntryAddressGetter addressGetter = new QueueEntryAddressGetter(id);
         log.trace("enqueue: Adding preCommitListener for Queue: " + id.toString());
         TransactionalContext.getRootContext().addPreCommitListener(addressGetter);

         corfuTable.insert(id, e);
         return id;
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
        @Setter
        private final CorfuRecordId recordId;

        @Getter
        private final ByteString entry;

        public String toString() {
            return String.format("%s=>%s", recordId, entry);
        }

        CorfuQueueRecord(CorfuRecordId recordId, ByteString entry) {
            this.recordId = recordId;
            this.entry = entry;
        }

        @Override
        public int compareTo(CorfuQueueRecord o) {
            return this.recordId.compareTo(o.getRecordId());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CorfuQueueRecord that = (CorfuQueueRecord) o;
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
    public List<CorfuQueueRecord> entryList(CorfuRecordId entriesAfter, int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("entryList can't take zero or negative maxEntries");
        }
        log.trace("entryList: "+maxEntries+" entries after:"+entriesAfter);

        List<CorfuQueueRecord> copy = new ArrayList<>(
                Math.min(corfuTable.size(), maxEntries)
        );

        Comparator<Map.Entry<CorfuRecordId, ByteString>> recordIdComparator = Comparator.comparing(Map.Entry::getKey);
        for (Map.Entry<CorfuRecordId, ByteString> entry : corfuTable.entryStream()
                .filter(e -> e.getKey().compareTo(entriesAfter) > 0)
                .sorted(recordIdComparator)
                .limit(maxEntries)
                .collect(Collectors.toList())) {
            copy.add(new CorfuQueueRecord(entry.getKey(), entry.getValue()));
        }
        return copy;
    }

    /**
     * @return all the entries in the Queue
     */
    public List<CorfuQueueRecord> entryList() {
        return this.entryList(new CorfuRecordId(0,0), Integer.MAX_VALUE);
    }

    /**
     * @param maxEntries limit number of entries returned to this.
     * @return all the entries in the Queue
     */
    public List<CorfuQueueRecord> entryList(int maxEntries) {
        return this.entryList(new CorfuRecordId(0, 0), maxEntries);
    }

    public boolean isEmpty() {
        return corfuTable.isEmpty();
    }

    public boolean containsKey(CorfuRecordId key) {
        return corfuTable.containsKey(key);
    }

    /**
     * Directly retrieve the enqueued element from the CorfuTable
     * @param key
     * @return
     */
    public ByteString get(CorfuRecordId key) {
        return corfuTable.get(key);
    }

    /**
     * Removes a specific element identified by the ID returned via enqueue() or entryList()'s CorfuRecord.
     *
     * @return The entry that was successfully removed or null if there was no mapping.
     */
    public void removeEntry(CorfuRecordId entryId) {
        corfuTable.delete(entryId);
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

        for (Iterator<Map.Entry<CorfuRecordId, ByteString>> it = corfuTable.entryStream().iterator(); it.hasNext(); ) {
            Map.Entry<CorfuRecordId, ByteString> entry = it.next();
            stringBuilder.append(entry.toString()).append(", ");
        }

        stringBuilder.append("}");
        return stringBuilder.toString();
    }
}
