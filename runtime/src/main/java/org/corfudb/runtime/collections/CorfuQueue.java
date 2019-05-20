package org.corfudb.runtime.collections;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.DontInstrument;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;

import java.util.*;

/** {@inheritDoc} */
@Slf4j
@CorfuObject
public class CorfuQueue<E> implements ICorfuQueue<E> {
    class EntryID implements ICorfuQueue.EntryId {
        UUID id = UUID.randomUUID();
        @Override
        public String toString() {
            return id.toString();
        }
    }

    /** The "main" linked map which contains the primary key-value mappings. */
    private final Map<EntryId,E> mainMap = new LinkedHashMap<>();

    /** {@inheritDoc} */
    @Override
    @Accessor
    public int size() {
        return mainMap.size();
    }

    /** {@inheritDoc} */
    @Override
    public EntryId enqueue(E e) {
        EntryId id = new EntryID();
        put(id, e);
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public List<Map.Entry<EntryId, E>> entryList(int maxEntires) {
        List<Map.Entry<EntryId, E>> copy = new ArrayList<>(maxEntires);
        for (Map.Entry<EntryId, E> entry : mainMap.entrySet()) {
            copy.add(new AbstractMap.SimpleImmutableEntry<>(entry.getKey(),
                    entry.getValue()));
            if (--maxEntires == 0) {
                break;
            }
        }
        return copy;
    }

    /** {@inheritDoc} */
    @Override
    public Map.Entry<EntryId, E> peek() {
        List<Map.Entry<EntryId, E>> copy = entryList(1);
        return copy.get(0);
    }

    /** {@inheritDoc} */
    @Accessor
    public boolean isEmpty() {
        return mainMap.isEmpty();
    }

    /** {@inheritDoc} */
    @Accessor
    public boolean containsKey(@ConflictParameter EntryId key) {
        return mainMap.containsKey(key);
    }

    /** {@inheritDoc} */
    @Accessor
    public E get(@ConflictParameter EntryId key) {
        return mainMap.get(key);
    }

    /** {@inheritDoc} */
    @MutatorAccessor(name = "put", undoFunction = "undoPut", undoRecordFunction = "undoPutRecord")
    protected E put(@ConflictParameter EntryId key, E value) {
        E previous = mainMap.put(key, value);
        return previous;
    }

    @DontInstrument
    protected E undoPutRecord(CorfuQueue<E> queue, EntryId key, E value) {
        return queue.mainMap.get(key);
    }

    @DontInstrument
    protected void undoPut(CorfuQueue<E> queue, E undoRecord, EntryId key, E entry) {
        // Same as undoRemove (restore previous value)
        undoRemove(queue, undoRecord, key);
    }

    @DontInstrument
    Object[] putAllConflictFunction(Map<? extends EntryId, ? extends E> m) {
        return m.keySet().stream()
                .map(Object::hashCode)
                .toArray(Object[]::new);
    }

    enum UndoNullable {
        NULL;
    }

    /** {@inheritDoc} */
    @Override
    @MutatorAccessor(name = "remove", undoFunction = "undoRemove",
                                undoRecordFunction = "undoRemoveRecord")
    @SuppressWarnings("unchecked")
    public E remove(@ConflictParameter EntryId key) {
        E previous =  mainMap.remove(key);
        return previous;
    }

    @DontInstrument
    protected E undoRemoveRecord(CorfuQueue<E> table, EntryId key) {
        return table.mainMap.get(key);
    }

    @DontInstrument
    protected void undoRemove(CorfuQueue<E> queue, E undoRecord, EntryId key) {
        if (undoRecord == null) {
            queue.mainMap.remove(key);
        } else {
            queue.mainMap.put(key, undoRecord);
        }
    }

    /** {@inheritDoc} */
    @Mutator(name = "clear", reset = true)
    public void clear() {
        mainMap.clear();
    }
}
