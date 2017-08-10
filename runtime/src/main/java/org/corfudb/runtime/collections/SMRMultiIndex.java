package org.corfudb.runtime.collections;


import com.google.common.collect.ImmutableSet;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.corfudb.annotations.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * //FIXME: These indexes are secondary indexes on a data structure. These should be more like a library that
 * // FIXME: extends the functionality of SMRMap and does not need to be a separate standalone datastructure.
 * This object implements a multi-indexed map.
 *
 * Maps are indexed by their row index, R, as well as multiple Named indexes
 * calculated at insert time using index functions, which generate
 * indexes from the inserted value.
 *
 * The Main Indexing Data Structure.
 * Each named index maintains a <Code>Map</Code> of indexKeys that in turn
 * point to a <Code>Map</Code> of keys and projection of values from the main <Code>Map</Code>
 * datastructure. This multi-level <Code>Map</Code> provides flexibility for the current use case.
 *
 *  ________________________________________________________________________________________________________
 * | <String>IndexName</String> -->  _______________________________________________________________________|
 * |                                 | <I>IndexKey1</I> --> ________________________________________________|
 * |                                 |                     |<K>PrimaryKey1</K> --><V>Projection(Value1)</V> |
 * |                                 |                     |<K>PrimaryKey2</K> --><V>Projection(Value2)</V> |
 * |                                 |                     |________________________________________________|
 * |                                 |                     |________________________________________________|
 * |                                 | <I>IndexKey2</I> -->|<K>PrimaryKey1</K> --><V>Projection(Value1)</V> |
 * |                                 |                     |<K>PrimaryKey3</K> --><V>Projection(Value3)</V> |
 * |_________________________________|_____________________|________________________________________________|
 *
 *
 *
 * Created by mwei on 4/7/17.
 */
//FIXME: having four generics is clunky. We need to come up with a simpler interface.
@CorfuObject
public class SMRMultiIndex<K, V, I, P> implements Map<K, V> {

    //TODO Index function is too generic and can be source of inefficiency
    //TODO Maybe there should be efficient implementations that can do efficient
    //TODO index generation.
    /**
     *
     * @param <K> The type of the key used in indexing.
     * @param <V> The type of the value used in indexing.
     * //@param <I> The type of index key.
     */
    @FunctionalInterface
    public interface IndexFunction<K, V, I>{
        I generateIndex(K k, V v);
    }

    /**
     * The interface for a projection function.
     * @param <K>   The type of the key used in projection.
     * @param <V>   The type of the value used in projection.
     * @param <P>   The type of the projection returned.
     */
    @FunctionalInterface
    public interface ProjectionFunction<K, V, P>{
        P generateProjection(K k, V v);
    }

    /**
     * Data holder for User to be able to specify the index creation.
     * It takes in a list of IndexFunction to index the main
     * Map in different ways.
     *
     * @param <K> The type of the key.
     * @param <V> The type of the value.
     * @param <I> The type of the index key.
     * @param <P> The type of projected value for the index
     */
    @Data
    @EqualsAndHashCode(of = {"name"})
    @AllArgsConstructor
    public static class IndexSpecification<K, V, I,P>{
        String name;
        private List<IndexFunction<K, V, I>> indexFunctions;
        private ProjectionFunction<K,V,P> projectionFunction;
    }

    /**
     * To Specify multiple named indexes for the Main Map.
     */
    final Map<String, IndexSpecification> indexSpecifications;

    /** The underlying HashMap. */
    final Map<K, V> mainMap;


    /**
     * The Main Indexing Data Structure.
     * Each named index maintains a <Code>Map</Code> of indexKeys that in turn
     * point to a <Code>Map</Code> of keys and projection of values from the main <Code>Map</Code>
     * datastructure. This multi-level <Code>Map</Code> provides flexibility for the current use case.
     *
     *  ________________________________________________________________________________________________________
     * | <String>IndexName</String> -->  _______________________________________________________________________|
     * |                                 | <I>IndexKey1</I> --> ________________________________________________|
     * |                                 |                     |<K>PrimaryKey1</K> --><V>Projection(Value1)</V> |
     * |                                 |                     |<K>PrimaryKey2</K> --><V>Projection(Value2)</V> |
     * |                                 |                     |________________________________________________|
     * |                                 |                     |________________________________________________|
     * |                                 | <I>IndexKey2</I> -->|<K>PrimaryKey1</K> --><V>Projection(Value1)</V> |
     * |                                 |                     |<K>PrimaryKey3</K> --><V>Projection(Value3)</V> |
     * |_________________________________|_____________________|________________________________________________|
     *
     *
     */
    /** Indexes for the main map.*/
    final Map<String, Map<I,Map<K,P>>> indexMaps;

    /** Create a new in-memory multi-index given
     * a set of indexing functions. */
    public SMRMultiIndex(List<IndexSpecification> indexSpecifications) {

        this.mainMap = new HashMap<>();
        this.indexSpecifications = new HashMap<>();
        indexMaps = new HashMap<String, Map<I,Map<K,P>>>();

        for(IndexSpecification indexSpecification: indexSpecifications){
            this.indexSpecifications.put(indexSpecification.getName(), indexSpecification);
            this.indexMaps.put(indexSpecification.getName(), new HashMap<I, Map<K,P>>());
        }
    }


    /** Put the object into the multi index, given the value and it's key.
     *
     * @param key    The rowKey to insert the object at.
     * @param value     The value to insert.
     * @return          The previous value at the row key.
     */

    @MutatorAccessor(name = "put",  undoFunction = "undoPut", undoRecordFunction = "undoPutRecord")
    //@MutatorAccessor(name = "put")
    @Override
    public V put(@ConflictParameter K key, V value) {

        V previous = mainMap.put(key, value);
        if(previous != null) {
            removeIndexes(key, previous);
        }
        createIndexes(key, value);
        return previous;
    }

    /** Generate an undo record for a put, given the previous state of the map
     * and the parameters to the put call.
     *
     * @param index             The previous state of the index
     * @param key               The key from the put call
     * @param value             The value from the put call. This is not
     *                          needed to generate an undo record.
     * @return                  An undo record, which for a put is the
     *                          previous value in the map.
     */
    @DontInstrument
    V undoPutRecord(SMRMultiIndex<K,V,I,P> index, K key, V value) {
        return index.get(key);
    }

    /** Undo a put, given the current state of the map, an undo record
     * and the arguments to the put command to undo.
     *
     * @param index           The state of the map after the put to undo
     * @param undoRecord    The undo record generated by undoPutRecord
     * @param key           The key of the put to undo
     * @param value         The value of the put to undo, which is not
     *                      needed.
     */
    @DontInstrument
    void undoPut(SMRMultiIndex<K,V,I,P> index, V undoRecord, K key, V value) {
        if (undoRecord == null) {
            index.remove(key);
        } else {
            index.put(key, undoRecord);
        }
    }

    @Accessor
    @Override
    public V get(Object key) {
        return mainMap.get(key);
    }



    @Override
    @MutatorAccessor(name = "remove", undoFunction = "undoRemove", undoRecordFunction = "undoRemoveRecord")
    //@MutatorAccessor(name = "remove")
    public V remove(Object key) {
        V previous = mainMap.remove(key);
        if(previous != null)
            removeIndexes((K)key, previous);
        return previous;
    }

    /** Generate an undo record for a remove, given the previous state of the map
     * and the parameters to the remove call.
     *
     * @param index             The previous state of the map
     * @param key               The key from the remove call
     * @return                  An undo record, which for a remove is the
     *                          previous value in the map.
     */
    @DontInstrument
    V undoRemoveRecord(SMRMultiIndex<K,V,I,P> index, K key) {
        return index.get(key);
    }

    /** Undo a remove, given the current state of the map, an undo record
     * and the arguments to the remove command to undo.
     *
     * @param index           The state of the map after the put to undo
     * @param undoRecord    The undo record generated by undoRemoveRecord
     */
    @DontInstrument
    void undoRemove(SMRMultiIndex<K,V,I,P> index, V undoRecord, K key) {
        if (undoRecord == null) {
            index.remove(key);
        } else {
            index.put(key, undoRecord);
        }
    }

    @Mutator(name = "putAll")
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

        throw new RuntimeException("###########Operation Not Implemented.##########");
    }


    /** Get a value by it's row key.
     *
     * @param key    The row key to retrieve.
     * @return          The object at the row key, or
     *                  NULL, if no object was mapped.
     */
    @Accessor
    @Nullable
    public V getByRowIndex(K key) {
        return mainMap.get(key);
    }


    /** Get a value by a given named index.
     *
     * @param indexName  The index of the index function from the list
     *                  passed in at construction time.
     * @param indexKey The index Key to use.
     * @return          A set of values which match the given index criteria.
     */
    @Accessor
    public Collection<Map.Entry<K,P>> getByNamedIndex(String indexName, I indexKey) {
        Map<I,Map<K,P>> indexMap = indexMaps.get(indexName);
        if (indexMap == null)
            return Collections.EMPTY_SET;
        Map<K,P> index = indexMap.get(indexKey);
        if (index == null)
            return Collections.EMPTY_SET;
        return index.entrySet();
    }

    /** Get the size of the multi-index.
     * @return  The number of entries mapped (rows).
     */
    @Accessor
    public int size() {
        return mainMap.size();
    }

    @Accessor
    @Override
    public boolean isEmpty() {
        return mainMap.isEmpty();
    }

    @Accessor
    @Override
    public boolean containsKey(Object key) {
        return mainMap.containsKey(key);
    }

    @Accessor
    @Override
    public boolean containsValue(Object value) {
        return mainMap.containsValue(value);
    }



    /** Clear the multi-index.
     */
    @Mutator(reset = true, name = "clear")
    public void clear() {
        throw new RuntimeException("###########Operation Not Implemented.##########");

    }

    @Accessor
    @Override
    public Set<K> keySet() {
        return mainMap.keySet();
    }

    @Accessor
    @Override
    public Collection<V> values() {
        return mainMap.values();
    }

    @Accessor
    @Override
    public Set<Entry<K, V>> entrySet() {
        return mainMap.entrySet();
    }


    //--------------- Helper Methods to Create/Update/Remove Indexes -----------

    public void createIndexes(K key, V value) {
        for(Map.Entry<String, IndexSpecification> entry: indexSpecifications.entrySet()){
            String indexName = entry.getKey();
            IndexSpecification indexSpecification = entry.getValue();
            List<Map<K, P>> indexList = getIndexList(indexName, indexSpecification, key, value);
            indexList.forEach(index -> index.put(key, (P)indexSpecification.projectionFunction.generateProjection(key, value)));
        }
    }
    public void removeIndexes(K key, V value) {
        for(Map.Entry<String, IndexSpecification> entry: indexSpecifications.entrySet()){
            String indexName = entry.getKey();
            IndexSpecification indexSpecification = entry.getValue();
            List<Map<K, P>> indexList = getIndexList(indexName, indexSpecification, key, value);
            indexList.forEach(index -> index.remove(key, (P)indexSpecification.projectionFunction.generateProjection(key, value)));
        }
    }

    public List<Map<K, P>> getIndexList(String indexName, IndexSpecification indexSpecification, K key, V value) {

        List<Map<K, P>> indexes = new ArrayList<Map<K, P>>();

        for (Object indexFunction : indexSpecification.getIndexFunctions()) {
            I indexKey = (I) ((IndexFunction<K, V, I>) indexFunction).generateIndex(key, value);
            Map<K, P> index = indexMaps.get(indexName).computeIfAbsent(indexKey, K -> new ConcurrentHashMap<>());
            indexes.add(index);
        }
        return indexes;
    }

}