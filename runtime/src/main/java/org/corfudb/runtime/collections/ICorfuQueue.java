package org.corfudb.runtime.collections;

import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.annotations.MutatorAccessor;

import java.util.List;
import java.util.Map;

/**
 * Persisted Queue supported by CorfuDB using distributed State Machine Replication.
 * Entries enqueued could be backed by a CorfuMap where each entry is mapped to a unique generated
 * id that is returned upon successful <b>enqueue()</b>.
 * Consumption is via the <b>entryList()</b> which returns entries along with their ids sorted by
 * the order in which their <b>enqueue()</b> operations materialized. Thus a logical FIFO queue.
 * Instead of a dequeue() this Queue supports a <b>remove()</b> which accepts the id of the element.
 * Entries cannot be modified in-place and but can be removed from anywhere.
 * Created by Sundar Sridharan on 5/8/19.
 * @param E entry to enqueue into the persisted queue
 */
public interface ICorfuQueue<E> extends ISMRObject {
    /**
     * {@inheritDoc}
     * Returns the size of the queue at a given point in time.
     *
     */
    @Accessor
    int size();

    /**
     * An abstract interface defining the identifier that will be used to uniquely
     * identify every entry in the Queue. The implementation could be anything from
     * a Java UUID to a unique 4 byte token.
     */
    interface EntryId {
        String toString();
    }

    /**
     * Appends the specified element at the end of this unbounded queue.
     * In a distributed system, the order of insertions cannot be guaranteed
     * unless a transaction is used.
     * Capacity restrictions and backoffs must be implemented outside this
     * interface. Consider validating the size of the queue against a high
     * watermark before enqueue.
     *
     * @param e the element to add
     * @throws NullPointerException if the specified element is null and this
     *         queue does not permit null elements
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     * @return EntryId representing this object in the persistent queue
     */
    @MutatorAccessor(name = "enqueue")
    EntryId enqueue(E e);

    /**
     * Returns a List of ID and entry pairs sorted by the order in which the enqueue materialized.
     * The ID is an unique identifier that can be used to remove entries that were enqueued.
     *
     * {@inheritDoc}
     *
     * <p>This function currently does not return a view like the java.util implementation,
     * and changes to the entryList will *not* be reflected in the map. </p>
     *
     * @param maxEntires - Limit the number of entries returned from start of the queue
     *                   -1 implies all entries
     * @throws IllegalArgumentException if in incorrect value is passed to maxEntries
     * @return List of Entries sorted by their enqueue order
     */
    @Accessor
    List<Map.Entry<EntryId, E>> entryList(int maxEntires);

    /**
     * Removes a specific element of this queue identified by the ID returned by entryList().
     *
     * {@inheritDoc}
     *
     * @throws NoSuchElementException if this queue did not contain this element
     * @return The entry that was successfully removed
     */
    @MutatorAccessor(name = "remove", undoFunction = "undoRemove",
            undoRecordFunction = "undoRemoveRecord")
    E remove(@ConflictParameter EntryId id);

    /**
     * Returns, without removing, the head of the logical queue.
     *
     * @return the element at the head of the queue.
     */
    @Accessor
    Map.Entry<EntryId, E> peek();
}

