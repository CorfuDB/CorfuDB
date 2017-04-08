package org.corfudb.runtime.collections;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.corfudb.annotations.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.IntStream;

/** This object implements a multi-indexed map.
 *
 * Maps are indexed by their row index, R, as well as multiple column indexes
 * calculated at insert time using a set of index functions, which generate
 * indexes from the inserted value.
 *
 * The map can be queried via the row index or by any column index. Multiple
 * values may exist at any given column index, and the implementation returns
 * a collection.
 *
 * Created by mwei on 4/7/17.
 */
@CorfuObject
public class SMRMultiIndex<R, V> implements ISMRObject {

    /** The interface for an index function.
     * @param <V>   The type of the object being indexed.
     */
    @FunctionalInterface
    interface IndexFunction<V> {
        Object generateIndex(V val);
    }

    /** A list of indexing functions. */
    final List<IndexFunction<V>> indexFunctions;

    /** The map indexed by the row key. */
    final Map<R, V> mainMap;

    /** The multi maps indexed by each column key. */
    final List<Multimap<Object,V>> indexList;

    /** Create a new in-memory multi-index given
     * a set of indexing functions. */
    public SMRMultiIndex(List<IndexFunction<V>> indexFunctions) {
        this.indexFunctions = indexFunctions;
        this.mainMap = new HashMap<>();
        this.indexList = new ArrayList<>();
        IntStream.range(0, indexFunctions.size())
                .forEach(i -> this.indexList.add(HashMultimap.create()));
    }

    /** Put the object into the multi index, given the value and it's rowkey.
     *
     * @param rowKey    The rowKey to insert the object at.
     * @param value     The value to insert.
     * @return          The previous value at the row key.
     */
    @MutatorAccessor(name = "put")
    void put(@ConflictParameter R rowKey, V value) {
        mainMap.put(rowKey, value);
        IntStream.range(0, indexFunctions.size())
                .forEach(i -> indexList.get(i).put(
                        indexFunctions.get(i).generateIndex(value), value));
    }

    /** Get a value by it's row key.
     *
     * @param rowKey    The row key to retrieve.
     * @return          The object at the row key, or
     *                  NULL, if no object was mapped.
     */
    @Accessor
    @Nullable V getByRowIndex(@ConflictParameter R rowKey) {
        return mainMap.get(rowKey);
    }

    /** Get a value by a given column key.
     *
     * @param indexNum  The index of the index function from the list
     *                  passed in at construction time.
     * @param columnKey The column index to use.
     * @return          A set of values which match the given column index.
     */
    @Accessor
    Collection<V> getByColumnIndex(int indexNum, Object columnKey) {
        return indexList.get(indexNum).get(columnKey);
    }

    /** Get the size of the multi-index.
     * @return  The number of entries mapped (rows).
     */
    @Accessor
    int size() {
        return mainMap.size();
    }

    /** Clear the multi-index.
     */
    @Mutator(reset = true)
    void clear() {
        mainMap.clear();
        IntStream.range(0, indexFunctions.size())
                .forEach(i -> indexList.get(i).clear());
    }

}
