package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.CRC32;
import lombok.Getter;
import org.corfudb.annotations.ConstructorType;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.ObjectType;
import org.corfudb.annotations.PassThrough;
import org.corfudb.annotations.TransactionalMethod;
import org.corfudb.runtime.object.AbstractCorfuWrapper;
import sun.misc.CRC16;

/**
 * Created by mwei on 3/29/16.
 */
@Deprecated // TODO: Add replacement method that conforms to style
@SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
@CorfuObject(constructorType = ConstructorType.PERSISTED,
        objectType = ObjectType.STATELESS)
public class FGMap<K, V> extends AbstractCorfuWrapper<FGMap<K,V>> implements Map<K, V> {

    @Getter
    public final int numBuckets;

    public FGMap(int numBuckets) {
        this.numBuckets = numBuckets;
    }

    public FGMap() {
        this.numBuckets = 10;
    }

    @PassThrough
    UUID getStreamID(int partition) {
        return new UUID(getStreamID().getMostSignificantBits(),
                getStreamID().getLeastSignificantBits() + (partition + 1));
    }

    @PassThrough
    Map<K, V> getPartitionMap(int partition) {
        return getBuilder()
                .setTypeToken(new TypeToken<SMRMap<K,V>>() {})
                .setStreamID(getStreamID(partition))
                .open();
    }

    /**
     * Get a new partition.
     *
     * <p>In order to avoid collisions due to imperfect hashCode() distribution,
     * we apply the Luby-Rackoff transform to randomize the distribution with
     * CRC32 and CRC16.
     *
     * @param key key
     * @return partition
     */
    @PassThrough
    int getPartitionNumber(Object key) {
        int baseMsb = key.hashCode() >> 16;
        int baseLsb = key.hashCode() & 0xFFFF;

        CRC16 crc16 = new CRC16();
        crc16.update((byte) (baseMsb & 0xFF));
        crc16.update((byte) (baseMsb >> 8));
        int hashCode1 = crc16.value & 0xFFFF;

        CRC32 crc32 = new CRC32();
        crc32.update(hashCode1 ^ baseLsb);
        int hashCode2 = (int) crc32.getValue();
        int hashCode = ((hashCode2 ^ baseMsb) << 16) | (hashCode1 ^ baseLsb);
        return Math.abs(hashCode % numBuckets);
    }

    @PassThrough
    Map<K, V> getPartition(Object key) {
        return getPartitionMap(getPartitionNumber(key));
    }

    @PassThrough
    List<Map<K, V>> getAllPartitionMaps() {
        return IntStream.range(0, numBuckets)
                .mapToObj(this::getPartitionMap)
                .collect(Collectors.toList());
    }

    @PassThrough
    Set<UUID> getAllStreamIDs() {
        return IntStream.range(0, numBuckets)
                .mapToObj(this::getStreamID)
                .collect(Collectors.toSet());
    }

    /**
     * Returns the number of key-value mappings in this map.  If the
     * map contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of key-value mappings in this map
     */
    @Override
    @TransactionalMethod(readOnly = true)
    public int size() {
        return getAllPartitionMaps().stream()
                .mapToInt(Map::size)
                .sum();
    }

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     *
     * @return <tt>true</tt> if this map contains no key-value mappings
     */
    @Override
    @TransactionalMethod(readOnly = true)
    public boolean isEmpty() {
        return getAllPartitionMaps().stream()
                .allMatch(Map::isEmpty);
    }

    /**
     * Returns <tt>true</tt> if this map contains a mapping for the specified
     * key.  More formally, returns <tt>true</tt> if and only if
     * this map contains a mapping for a key <tt>k</tt> such that
     * <tt>(key==null ? k==null : key.equals(k))</tt>.  (There can be
     * at most one such mapping.)
     *
     * @param key key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified key
     * @throws ClassCastException   if the key is of an inappropriate type for
     *                              this map
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified key is null and this map
     *                              does not permit null keys
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    @PassThrough
    public boolean containsKey(Object key) {
        return getPartition(key)
                .containsKey(key);
    }

    /**
     * Returns <tt>true</tt> if this map maps one or more keys to the
     * specified value.  More formally, returns <tt>true</tt> if and only if
     * this map contains at least one mapping to a value <tt>v</tt> such that
     * <tt>(value==null ? v==null : value.equals(v))</tt>.  This operation
     * will probably require time linear in the map size for most
     * implementations of the <tt>Map</tt> interface.
     *
     * @param value value whose presence in this map is to be tested
     * @return <tt>true</tt> if this map maps one or more keys to the specified value
     * @throws ClassCastException   if the value is of an inappropriate type for
     *                              this map
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified value is null and this
     *                              map does not permit null values
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    @TransactionalMethod(readOnly = true)
    public boolean containsValue(Object value) {
        return getAllPartitionMaps().stream()
                .anyMatch(x -> x.containsValue(value));
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code (key==null ? k==null :
     * key.equals(k))}, then this method returns {@code v}; otherwise
     * it returns {@code null}.  (There can be at most one such mapping.)
     *
     * <p>If this map permits null values, then a return value of
     * {@code null} does not <i>necessarily</i> indicate that the map
     * contains no mapping for the key; it's also possible that the map
     * explicitly maps the key to {@code null}.  The {@link #containsKey
     * containsKey} operation may be used to distinguish these two cases.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or
     * {@code null} if this map contains no mapping for the key
     * @throws ClassCastException   if the key is of an inappropriate type for
     *                              this map
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException if the specified key is null and this map
     *                              does not permit null keys
     *                              (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    @PassThrough
    public V get(Object key) {
        return getPartition(key).get(key);
    }

    /**
     * Associates the specified value with the specified key in this map
     * (optional operation).  If the map previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A map
     * <tt>m</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(Object) m.containsKey(k)} would return
     * <tt>true</tt>.)
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or <tt>null</tt>
     *         if there was no mapping for <tt>key</tt>. (A <tt>null</tt> return
     *         can also indicate that the map previously associated <tt>null</tt>
     *         with <tt>key</tt>, if the implementation supports <tt>null</tt> values.)
     * @throws UnsupportedOperationException if the <tt>put</tt> operation
     *                                       is not supported by this map
     * @throws ClassCastException            if the class of the specified key or value
     *                                       prevents it from being stored in this map
     * @throws NullPointerException          if the specified key or value is null
     *                                       and this map does not permit null keys or values
     * @throws IllegalArgumentException      if some property of the specified key
     *                                       or value prevents it from being stored in this map
     */
    @Override
    @PassThrough
    public V put(K key, V value) {
        return getPartition(key).put(key, value);
    }

    /**
     * Removes the mapping for a key from this map if it is present
     * (optional operation).   More formally, if this map contains a mapping
     * from key <tt>k</tt> to value <tt>v</tt> such that
     * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping
     * is removed.  (The map can contain at most one such mapping.)
     *
     * <p>Returns the value to which this map previously associated the key,
     * or <tt>null</tt> if the map contained no mapping for the key.
     *
     * <p>If this map permits null values, then a return value of
     * <tt>null</tt> does not <i>necessarily</i> indicate that the map
     * contained no mapping for the key; it's also possible that the map
     * explicitly mapped the key to <tt>null</tt>.
     *
     * <p>The map will not contain a mapping for the specified key once the
     * call returns.
     *
     * @param key key whose mapping is to be removed from the map
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @throws UnsupportedOperationException if the <tt>remove</tt> operation
     *                                       is not supported by this map
     * @throws ClassCastException            if the key is of an inappropriate type for
     *                                       this map
     *                                       (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException          if the specified key is null and this
     *                                       map does not permit null keys
     *                                       (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
     */
    @Override
    @PassThrough
    public V remove(Object key) {
        return getPartition(key).remove(key);
    }

    /**
     * Copies all of the mappings from the specified map to this map
     * (optional operation).  The effect of this call is equivalent to that
     * of calling {@link #put(Object, Object) put(k, v)} on this map once
     * for each mapping from key <tt>k</tt> to value <tt>v</tt> in the
     * specified map.  The behavior of this operation is undefined if the
     * specified map is modified while the operation is in progress.
     *
     * @param m mappings to be stored in this map
     * @throws UnsupportedOperationException if the <tt>putAll</tt> operation
     *                                       is not supported by this map
     * @throws ClassCastException            if the class of a key or value in the
     *                                       specified map prevents it from being stored in this map
     * @throws NullPointerException          if the specified map is null, or if
     *                                       this map does not permit null keys or values, and the
     *                                       specified map contains null keys or values
     * @throws IllegalArgumentException      if some property of a key or value in the specified
     *                                       map prevents it from being stored in this map
     */
    @Override
    @TransactionalMethod(modifiedStreamsFunction = "putAllGetStreams")
    public void putAll(Map<? extends K, ? extends V> m) {
        m.entrySet().stream()
                .forEach(e -> getPartition(e.getKey()).put(e.getKey(), e.getValue()));
    }

    /**
     * Get the set of streams which will be touched by this put all operation.
     *
     * @param m The map used for the putAll operation
     * @return A set of stream IDs
     */
    Set<UUID> putAllGetStreams(Map<? extends K, ? extends V> m) {
        return m.keySet().stream()
                .map(this::getPartitionNumber)
                .distinct()
                .map(this::getStreamID)
                .collect(Collectors.toSet());
    }

    /**
     * Removes all of the mappings from this map (optional operation).
     * The map will be empty after this call returns.
     *
     * @throws UnsupportedOperationException if the <tt>clear</tt> operation
     *                                       is not supported by this map
     */
    @Override
    @TransactionalMethod(modifiedStreamsFunction = "getAllStreamIDs")
    public void clear() {
        getAllPartitionMaps()
                .forEach(Map::clear);
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation), the results of
     * the iteration are undefined.  The set supports element removal,
     * which removes the corresponding mapping from the map, via the
     * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
     * operations.  It does not support the <tt>add</tt> or <tt>addAll</tt>
     * operations.
     *
     * @return a set view of the keys contained in this map
     */
    @Override
    @TransactionalMethod(readOnly = true)
    public Set<K> keySet() {
        return getAllPartitionMaps().stream()
                .map(Map::keySet)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  If the map is
     * modified while an iteration over the collection is in progress
     * (except through the iterator's own <tt>remove</tt> operation),
     * the results of the iteration are undefined.  The collection
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Collection.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt> and <tt>clear</tt> operations.  It does not
     * support the <tt>add</tt> or <tt>addAll</tt> operations.
     *
     * @return a collection view of the values contained in this map
     */
    @Override
    @TransactionalMethod(readOnly = true)
    public Collection<V> values() {
        return getAllPartitionMaps().stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation, or through the
     * <tt>setValue</tt> operation on a map entry returned by the
     * iterator) the results of the iteration are undefined.  The set
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt> and
     * <tt>clear</tt> operations.  It does not support the
     * <tt>add</tt> or <tt>addAll</tt> operations.
     *
     * @return a set view of the mappings contained in this map
     */
    @Override
    @TransactionalMethod(readOnly = true)
    public Set<Entry<K, V>> entrySet() {
        return getAllPartitionMaps().stream()
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }
}
