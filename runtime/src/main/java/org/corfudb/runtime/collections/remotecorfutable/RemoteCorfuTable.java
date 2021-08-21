package org.corfudb.runtime.collections.remotecorfutable;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.view.stream.AddressMapStreamView;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RemoteCorfuTable<K,V> implements ICorfuTable<K,V>, AutoCloseable {
    private final RemoteCorfuTableAdapter<K,V> adapter;

    @Getter
    private final String tableName;
    @Getter
    private final UUID streamId;

    @Override
    public void insert(K key, V value) {
        adapter.update(key, value);
    }

    @Override
    public void delete(K key) {
        adapter.delete(key);
    }

    public void multiDelete(List<K> keys) {
        adapter.multiDelete(keys);
    }

    public List<RemoteCorfuTableEntry<K,V>> scanFromBeginning() {
        return adapter.scan(adapter.getCurrentTimestamp());
    }

    public List<RemoteCorfuTableEntry<K,V>> scanFromBeginning(int numEntries) {
        return adapter.scan(numEntries, adapter.getCurrentTimestamp());
    }

    public List<RemoteCorfuTableEntry<K,V>> cursorScan(K startPoint) {
        return adapter.scan(startPoint, adapter.getCurrentTimestamp());
    }

    public List<RemoteCorfuTableEntry<K,V>> cursorScan(K startPoint, int numEntries) {
        return adapter.scan(startPoint, numEntries, adapter.getCurrentTimestamp());
    }

    @Override
    public List<V> scanAndFilter(Predicate<? super V> valuePredicate) {
        return scanAndFilterFromBeginning(valuePredicate);
    }

    public List<V> scanAndFilterFromBeginning(Predicate<? super V> valuePredicate) {
        List<RemoteCorfuTableEntry<K,V>> scannedEntries = adapter.scan(adapter.getCurrentTimestamp());
        return filteredValues(valuePredicate, scannedEntries);
    }

    public List<V> scanAndFilterFromBeginning(int numEntries, Predicate<? super V> valuePredicate) {
        List<RemoteCorfuTableEntry<K,V>> scannedEntries = adapter.scan(numEntries, adapter.getCurrentTimestamp());
        return filteredValues(valuePredicate, scannedEntries);
    }

    public List<V> cursorScanAndFilter(K startPoint, Predicate<? super V> valuePredicate) {
        List<RemoteCorfuTableEntry<K,V>> scannedEntries = adapter.scan(startPoint, adapter.getCurrentTimestamp());
        return filteredValues(valuePredicate, scannedEntries);
    }

    public List<V> cursorScanAndFilter(K startPoint, int numEntries, Predicate<? super V> valuePredicate) {
        List<RemoteCorfuTableEntry<K,V>> scannedEntries = adapter.scan(startPoint, numEntries, adapter.getCurrentTimestamp());
        return filteredValues(valuePredicate, scannedEntries);
    }

    private List<V> filteredValues(Predicate<? super V> valuePredicate, List<RemoteCorfuTableEntry<K, V>> scannedEntries) {
        return scannedEntries.stream()
                .filter(entry -> valuePredicate.test(entry.getValue()))
                .map(RemoteCorfuTableEntry::getValue)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<Entry<K, V>> scanAndFilterByEntry(Predicate<? super Entry<K, V>> entryPredicate) {
        return scanAndFilterByEntryFromBeginning(entryPredicate);
    }

    public Collection<Entry<K, V>> scanAndFilterByEntryFromBeginning(Predicate<? super Entry<K, V>> entryPredicate) {
        List<RemoteCorfuTableEntry<K,V>> scannedEntries = adapter.scan(adapter.getCurrentTimestamp());
        return filteredEntries(entryPredicate, scannedEntries);
    }

    public Collection<Entry<K, V>> scanAndFilterByEntryFromBeginning(int numEntries, Predicate<? super Entry<K, V>> entryPredicate) {
        List<RemoteCorfuTableEntry<K,V>> scannedEntries = adapter.scan(numEntries, adapter.getCurrentTimestamp());
        return filteredEntries(entryPredicate, scannedEntries);
    }

    public Collection<Entry<K, V>> cursorScanAndFilterByEntry(K startPoint, Predicate<? super Entry<K, V>> entryPredicate) {
        List<RemoteCorfuTableEntry<K,V>> scannedEntries = adapter.scan(startPoint, adapter.getCurrentTimestamp());
        return filteredEntries(entryPredicate, scannedEntries);
    }

    public Collection<Entry<K, V>> cursorScanAndFilterByEntry(K startPoint, int numEntries, Predicate<? super Entry<K, V>> entryPredicate) {
        List<RemoteCorfuTableEntry<K,V>> scannedEntries = adapter.scan(startPoint, numEntries, adapter.getCurrentTimestamp());
        return filteredEntries(entryPredicate, scannedEntries);
    }

    private List<Entry<K, V>> filteredEntries(Predicate<? super Entry<K, V>> entryPredicate,
                                              List<RemoteCorfuTableEntry<K, V>> scannedEntries) {
        return scannedEntries.stream().filter(entryPredicate).collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     * <p>
     *     TODO: link scan impl
     *     Please use scan instead.
     * </p>
     */
    @Override
    public Stream<Entry<K, V>> entryStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return adapter.size(adapter.getCurrentTimestamp());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return adapter.containsKey((K) key, adapter.getCurrentTimestamp());
    }

    @Override
    public boolean containsValue(Object value) {
        return adapter.containsValue((V) value, adapter.getCurrentTimestamp());
    }

    @Override
    public V get(Object key) {
        return adapter.get((K) key, adapter.getCurrentTimestamp());
    }

    public List<RemoteCorfuTableEntry<K,V>> multiGet(List<K> keys) {
        return adapter.multiGet(keys, adapter.getCurrentTimestamp());
    }

    @Override
    public V put(K key, V value) {
        //synchronization guarantees unneeded as timestamp will define versioning
        V returnVal = adapter.get(key, adapter.getCurrentTimestamp());
        adapter.update(key, value);
        return returnVal;
    }

    @Override
    public V remove(Object key) {
        //synchronization guarantees unneeded as timestamp will define versioning
        V returnVal = adapter.get((K) key, adapter.getCurrentTimestamp());
        adapter.delete((K) key);
        return returnVal;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        adapter.updateAll(m.entrySet().stream()
                .map(entry -> new RemoteCorfuTableEntry<K,V>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));
    }

    public void updateAll(Collection<RemoteCorfuTableEntry<K,V>> entries) {
        adapter.updateAll(entries);
    }

    @Override
    public void clear() {
        adapter.clear();
    }

    /**
     * {@inheritDoc}
     * <p>
     *     TODO: link scan impl
     *     Please use scan instead.
     * </p>
     */
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * <p>
     *     TODO: link scan impl
     *     Please use scan instead.
     * </p>
     */
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * <p>
     *     TODO: link scan impl
     *     Please use scan instead.
     * </p>
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        //TODO: close adapter and deregister table
        adapter.close();
    }

    @AllArgsConstructor
    public static class RemoteCorfuTableEntry<K,V> implements Map.Entry<K,V> {

        private final K key;
        private final V value;

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        /**
         * {@inheritDoc}
         * <p>
         *     Since the data for the table is stored on server side, please perform updates to the
         *     map through update.
         * </p>
         */
        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RemoteCorfuTableEntry<?, ?> entry = (RemoteCorfuTableEntry<?, ?>) o;
            return Objects.equals(key, entry.key) && Objects.equals(value, entry.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    public static class RemoteCorfuTableFactory {
        private RemoteCorfuTableFactory() {}

        public static <I,J> RemoteCorfuTable<I,J> openTable(@NonNull CorfuRuntime runtime, @NonNull String tableName) {
            UUID streamID = UUID.nameUUIDFromBytes(tableName.getBytes(StandardCharsets.UTF_8));
            ISerializer serializer = Serializers.getDefaultSerializer();
            IStreamView streamView = new AddressMapStreamView(runtime, streamID);
            RemoteCorfuTableAdapter<I,J> adapter = new RemoteCorfuTableAdapter<>(tableName, streamID, runtime,
                    serializer, streamView);
            return new RemoteCorfuTable<>(adapter, tableName, streamID);
        }
    }
}
