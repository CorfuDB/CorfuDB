package org.corfudb.runtime.collections.remotecorfutable;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.view.stream.AddressMapStreamView;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
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

    /**
     * Returns a table scanner with no result filters.
     * @return No filter table scanner.
     */
    public Scanner getScanner() {
        return new Scanner();
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Please use {@link #getValueFilterScanner(Predicate)} instead.
     * </p>
     */
    @Override
    public List<V> scanAndFilter(Predicate<? super V> valuePredicate) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a table scanner that filters results over the given value predicate.
     * @param valuePredicate Predicate with which to filter scanned values.
     * @return Value filter table scanner.
     */
    public Scanner getValueFilterScanner(Predicate<? super V> valuePredicate) {
        return new Scanner(valuePredicate, false);
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Please use {@link #getEntryFilterScanner(Predicate)} instead.
     * </p>
     */
    @Override
    public Collection<Entry<K, V>> scanAndFilterByEntry(Predicate<? super Entry<K, V>> entryPredicate) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a table scanner that filters results over the given entry predicate.
     * @param entryPredicate Predicate with which to filter scanned entries.
     * @return Entry filter table scanner.
     */
    public Scanner getEntryFilterScanner(Predicate<? super Entry<K, V>> entryPredicate) {
        return new Scanner(entryPredicate, true);
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Please use {@link #getScanner()} instead.
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

    public List<TableEntry<K,V>> multiGet(List<K> keys) {
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
                .map(entry -> new TableEntry<K,V>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));
    }

    public void updateAll(Collection<TableEntry<K,V>> entries) {
        adapter.updateAll(entries);
    }

    @Override
    public void clear() {
        adapter.clear();
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Please use {@link #getScanner()} instead.
     * </p>
     */
    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Please use {@link #getScanner()} instead.
     * </p>
     */
    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * <p>
     *     Please use {@link #getScanner()} instead.
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
    public static class TableEntry<K,V> implements Map.Entry<K,V> {

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
            TableEntry<?, ?> entry = (TableEntry<?, ?>) o;
            return Objects.equals(key, entry.key) && Objects.equals(value, entry.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    /**
     * This class is used to perform cursor scans of the RemoteCorfuTable.
     */
    public class Scanner {
        @Getter
        private boolean finished = false;

        //Invariant: scanner can not have both predicates
        private final Predicate<? super Entry<K, V>> entryPredicate;
        private final Predicate<? super V> valuePredicate;

        private List<TableEntry<K,V>> currentResultsEntries = null;
        private List<V> currentResultsValues = null;
        private K cursor = null;

        /**
         * Creates scanner with no filters.
         */
        private Scanner() {
            entryPredicate = null;
            valuePredicate = null;
        }

        /**
         * Creates scanner with specified predicate.
         * @param predicate Predicate with which to filter results.
         * @param isEntryPredicate True, if given predicate filters over entries.
         */
        private Scanner(Predicate predicate, boolean isEntryPredicate) {
            if (isEntryPredicate) {
                entryPredicate = predicate;
                valuePredicate = null;
            } else {
                entryPredicate = null;
                valuePredicate = predicate;
            }
        }

        /**
         * Get the entries read from the previous scan.
         * @return List of TableEntries from the previous scan.
         */
        public List<TableEntry<K,V>> getCurrentResultsEntries() {
            if (currentResultsEntries == null) {
                log.warn("Attempted to get results from unstarted scan");
                return new LinkedList<>();
            } else {
                return currentResultsEntries;
            }
        }

        /**
         * Get the values read from the previous scan.
         * @return List of Values from the previous scan.
         */
        public List<V> getCurrentResultsValues() {
            if (currentResultsValues != null) {
                return currentResultsValues;
            } else if (currentResultsEntries != null) {
                currentResultsValues = currentResultsEntries.stream()
                        .map(TableEntry::getValue).collect(Collectors.toList());
                return currentResultsValues;
            } else {
                log.warn("Attempted to get results from unstarted scan");
                return new LinkedList<>();
            }
        }

        /**
         * Perform a scan. If the Scanner is finished, this method will not modify any results.
         */
        public void getNextResults() {
            if (finished) {
                log.warn("Attempted to scan on a finished scanner instance");
                return;
            }
            List<TableEntry<K,V>> scannedEntries;
            if (cursor != null) {
                scannedEntries = adapter.scan(cursor, adapter.getCurrentTimestamp());
            } else {
                scannedEntries = adapter.scan(adapter.getCurrentTimestamp());
            }
            handleResults((List<TableEntry<K, V>>) scannedEntries);
        }

        /**
         * Perform a scan requesting the specified amount of results.
         * @param numResults The desired amount of results from the scan.
         */
        public void getNextResults(int numResults) {
            if (finished) {
                log.warn("Attempted to scan on a finished scanner instance");
                return;
            }
            List<TableEntry<K,V>> scannedEntries;
            if (cursor != null) {
                scannedEntries = adapter.scan(cursor, numResults, adapter.getCurrentTimestamp());
            } else {
                scannedEntries = adapter.scan(numResults, adapter.getCurrentTimestamp());
            }
            handleResults(scannedEntries);
        }

        private void handleResults(List<TableEntry<K, V>> scannedEntries) {
            if (scannedEntries.isEmpty()) {
                finished = true;
                currentResultsEntries = scannedEntries;
            } else {
                cursor = scannedEntries.get(scannedEntries.size() - 1).getKey();
                currentResultsEntries = filter(scannedEntries);
            }
            currentResultsValues = null;
        }

        private List<TableEntry<K,V>> filter(List<TableEntry<K,V>> scannedEntries) {
            if (entryPredicate != null) {
                return scannedEntries.stream().filter(entryPredicate).collect(Collectors.toList());
            } else if (valuePredicate != null) {
                return scannedEntries.stream()
                        .filter(entry -> valuePredicate.test(entry.getValue()))
                        .collect(Collectors.toList());
            } else {
                return scannedEntries;
            }
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
