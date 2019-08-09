package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.CorfuGuidGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

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
    private final CorfuTable<Long, E> corfuTable;
    private final CorfuTable<Long, Long> orderTable;
    private final CorfuRuntime runtime;
    private final CorfuGuidGenerator guidGenerator;
    private final Long GLOBAL_ORDER_KEY = 0L;

    public CorfuQueue(CorfuRuntime runtime, String streamName) {
        this.runtime = runtime;
        corfuTable = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Long, E>>() {})
                .setStreamName(streamName)
                .setArguments(CorfuTable.IndexRegistry.empty(), new LinkedHashMap<Long, E>())
                .open();
        orderTable = runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {})
                .setStreamName(streamName+"ORDER")
                .open();
        guidGenerator = new CorfuGuidGenerator(runtime);
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
     * with their snapshot+index id (also a long) which represents a global comparable ordering.
     * This class encapsulates these two longs into one Id and add rules on comparability.
     */
    public static class CorfuRecordId implements Comparable<CorfuRecordId> {
        private final UUID id;
        public CorfuRecordId(long ordering, long uniqueId) {
            this.id = new UUID(ordering, uniqueId);
        }

        /**
         * @return Return only the unique part of the id without the ordering
         */
        public long getEntryId() {
            return id.getLeastSignificantBits();
        }

        /**
         * @return Return only the ordering part of the entry without the id.
         */
        public long getOrdering() {
            return id.getMostSignificantBits();
        }

        /**
         * It's NOT ok to compare two objects if their ordering metadata is dissimilar.
         * @param o object to compare against.
         * @throws IllegalArgumentException if the two Ids are not comparable.
         * @return results of comparison.
         */
        @Override
        public int compareTo(CorfuRecordId o) {
            if (this.id.getMostSignificantBits() == 0 && o.id.getMostSignificantBits() != 0) {
                throw new IllegalArgumentException(
                        "Incompatible CorfuRecordId comparison: ordering unavailable");
            }
            if (this.id.getMostSignificantBits() !=0 && o.id.getMostSignificantBits() == 0) {
               throw new IllegalArgumentException(
                       "Incompatible CorfuRecordId comparison: order of compared object unknown");
            }
            if (this.id.getLeastSignificantBits() == o.id.getLeastSignificantBits()) {
                return 0;
            }
            return id.compareTo(o.id);
        }

        /**
         * It is ok to check equality of a CorfuRecordId with ordering data against one without.
         * @param o object to compare against.
         * @return
         */
        public boolean equals(CorfuRecordId o) {
            return id.getLeastSignificantBits() == o.id.getLeastSignificantBits();
        }

        public String toString() {
            return id.toString();
        }
    }

    /**
     * Appends the specified element at the end of this unbounded queue.
     * In a distributed system, the linearizable order of insertions cannot be guaranteed
     * unless a transaction is used.
     * Capacity restrictions and backoffs must be implemented outside this
     * interface. Consider validating the size of the queue against a high
     * watermark before enqueue.
     *
     * @param e the element to add
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     * @return Unique ID representing this entry in the persistent queue
     * WARNING: The ID returned by this function will NOT be the comparable with the ID
     *          returned by the CorfuQueueRecord from entryList() method because if this
     *          method executes within a transaction, the comparable ordering is not
     *          known until the transaction commits.
     *          The ID returned here is only really useful for remove() operations.
     */
    public CorfuRecordId enqueue(E e) {
        final Long id = guidGenerator.nextLong();
        corfuTable.put(id, e);
        return new CorfuRecordId(0, id);
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
         * However it cannot be compared with ID returned from the enqueue method for ordering
         * because if this method is wrapped in a transaction, the order is established only later.
         */
        @Getter
        private final CorfuRecordId recordId;

        @Getter
        private final E entry;

        public String toString() {
            return String.format("%s=>%s", recordId, entry);
        }

        CorfuQueueRecord(long ordering, long entryId, E entry) {
            this.recordId = new CorfuRecordId(ordering, entryId);
            this.entry = entry;
        }

        @Override
        public int compareTo(CorfuQueueRecord<? extends E> o) {
            return this.recordId.compareTo(o.getRecordId());
        }
    }

    /**
     * We need to encode 2 pieces of information into an 8 byte long to represent ordering.
     * 1. We use 40 bits for CorfuQueue's snapshot version information.
     * 2. We use 24 bits for the index within that CorfuQueue's snapshot.
     */
    final private static int MAX_BITS_FOR_INDEX = 24;
    final private static int MAX_INDEX_ENTRIES = (1<<MAX_BITS_FOR_INDEX) - 1;

    /**
     * Returns a List of CorfuQueueRecords sorted by the order in which the enqueue materialized.
     * This is the primary method of consumption of entries enqueued into CorfuQueue.
     *
     * To re-constitute the commit order in the presence of transactions the CorfuRecords returned
     * here have their UUID fields composed of two parts:
     *     +------------------------------------------------------------------+
     *     | Global index in the list |     ID of the entry in the map        |
     *     +------------------------------------------------------------------+
     *     <---- 8 bytes--------------><------- 8 bytes ---------------------->
     *
     *
     * <p>This function currently does not return a view like the java.util implementation,
     * and changes to the entryList will *not* be reflected in the map. </p>
     *
     * @param maxEntries - Limit the number of entries returned from start of the queue
     * @throws IllegalArgumentException if maxEntries is negative.
     * @return List of Entries sorted by their enqueue order
     */
    public List<CorfuQueueRecord<E>> entryList(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("entryList given negative maxEntries");
        }
        if (maxEntries > MAX_INDEX_ENTRIES) {
            throw new IllegalArgumentException(
                    "entryList can't return more than "+MAX_INDEX_ENTRIES+" entries"
            );
        }
        List<CorfuQueueRecord<E>> copy = Collections.emptyList();
        boolean startedNewTransaction = false;
        do {
            try {
                if (!TransactionalContext.isInTransaction()) {
                    runtime.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE)
                            .build()
                            .begin();
                    startedNewTransaction = true;
                }
                Long maxOrder = orderTable.getOrDefault(GLOBAL_ORDER_KEY, 0L);

                copy = new ArrayList<>(
                        Math.min(corfuTable.size(), maxEntries)
                );
                int index = 0;
                for (Map.Entry<Long, E> entry : corfuTable.entrySet()) {
                    if (++index >= maxEntries) {
                        break;
                    }
                    long entryId = entry.getKey();
                    Long ordering = orderTable.get(entryId);
                    if (ordering == null) {
                        ordering = maxOrder + index;
                        orderTable.putIfAbsent(entryId, ordering);
                    }
                    CorfuQueueRecord<E> record = new CorfuQueueRecord<>(
                            ordering, entryId, entry.getValue()
                    );
                    copy.add(record);
                }
                orderTable.put(GLOBAL_ORDER_KEY, maxOrder + index);

                if (startedNewTransaction) {
                    runtime.getObjectsView().TXEnd();
                    startedNewTransaction = false;
                }
            } catch (TransactionAbortedException e) {
                if (e.getAbortCause() != AbortCause.CONFLICT) {
                    throw e;
                }
            }
        } while (startedNewTransaction);
        return copy;
    }

    /**
     * @return all the entries in the Queue
     */
    public List<CorfuQueueRecord<E>> entryList() {
        return this.entryList(MAX_INDEX_ENTRIES);
    }

    public boolean isEmpty() {
        return corfuTable.isEmpty();
    }

    public boolean containsKey(CorfuRecordId key) {
        return corfuTable.containsKey(key.id.getLeastSignificantBits());
    }

    /**
     * Directly retrieve the enqueued element from the CorfuTable
     * @param key
     * @return
     */
    public E get(CorfuRecordId key) {
        return corfuTable.get(key.id.getLeastSignificantBits());
    }

    /**
     * Removes a specific element identified by the ID returned via enqueue() or entryList()'s CorfuRecord.
     *
     * @return The entry that was successfully removed or null if there was no mapping.
     */
    public E removeEntry(CorfuRecordId entryId) {
        orderTable.remove(entryId.getEntryId());
        return corfuTable.remove(entryId.getEntryId());
    }

    /**
     * Remove all entries from the Queue.
     */
    public void clear() {
        orderTable.clear();
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
        for (Map.Entry<Long, E> entry : corfuTable.entrySet()) {
            stringBuilder.append(entry.toString()).append(", ");
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }
}
