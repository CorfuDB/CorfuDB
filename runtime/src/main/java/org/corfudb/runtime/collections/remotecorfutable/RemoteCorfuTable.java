package org.corfudb.runtime.collections.remotecorfutable;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import static org.corfudb.runtime.CorfuRuntime.getCheckpointStreamIdFromId;
import org.corfudb.runtime.collections.CorfuTable;
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
import java.util.Optional;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void insert(K key, V value) {
        adapter.update(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(K key) {
        adapter.delete(key);
    }

    /**
     * Atomically removes a set of keys from the table.
     * @param keys The keys to remove from the table.
     */
    public void multiDelete(List<K> keys) {
        adapter.multiDelete(keys);
    }

    /**
     * Returns a table scanner with no result filters.
     * @return No filter table scanner.
     */
    public Scanner getScanner() {
        return new Scanner(false, Optional.empty(), Optional.empty(), Optional.empty(),
                adapter.getCurrentTimestamp());
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
        return new ValueFilteringScanner(false, Optional.empty(), Optional.empty(), Optional.empty(),
                adapter.getCurrentTimestamp(), valuePredicate);
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
        return new EntryFilteringScanner(false, Optional.empty(), Optional.empty(), Optional.empty(),
                adapter.getCurrentTimestamp(), entryPredicate);
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

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(Object key) {
        return adapter.containsKey((K) key, adapter.getCurrentTimestamp());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(Object value) {
        return adapter.containsValue((V) value, adapter.getCurrentTimestamp());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(Object key) {
        return adapter.get((K) key, adapter.getCurrentTimestamp());
    }

    /**
     * Reads the current value of all requested keys. Entries may not be returned in the same order as the keys.
     * @param keys The keys to get from the table.
     * @return List of entries, each entry corresponds to one requested key.
     */
    public List<TableEntry<K,V>> multiGet(List<K> keys) {
        return adapter.multiGet(keys, adapter.getCurrentTimestamp());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(K key, V value) {
        //synchronization guarantees unneeded as timestamp will define versioning
        V returnVal = adapter.get(key, adapter.getCurrentTimestamp());
        adapter.update(key, value);
        return returnVal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(Object key) {
        //synchronization guarantees unneeded as timestamp will define versioning
        V returnVal = adapter.get((K) key, adapter.getCurrentTimestamp());
        adapter.delete((K) key);
        return returnVal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        adapter.updateAll(m.entrySet().stream()
                .map(entry -> new TableEntry<K,V>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));
    }

    /**
     * Atomically updates table with all given entries.
     * @param entries The entries to add to the table.
     */
    public void updateAll(Collection<TableEntry<K,V>> entries) {
        adapter.updateAll(entries);
    }

    /**
     * {@inheritDoc}
     */
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
        //TODO: deregister table
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

    /*--------------------------------------------Scanners------------------------------------------------*/

    /**
     * This class is the superclass for all cursor scans of the RemoteCorfuTable.
     * Performs the scan operation with no filters.
     */
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public class Scanner {
        @Getter
        private final boolean finished;
        private final Optional<ImmutableList<TableEntry<K,V>>> currentResultsEntries;
        private final Optional<ImmutableList<V>> currentResultsValues;
        private final Optional<K> cursor;
        protected final long timestamp;

        /**
         * Get the entries read from the previous scan.
         * @return List of TableEntries from the previous scan.
         */
        public List<RemoteCorfuTable.TableEntry<K,V>> getCurrentResultsEntries() {
            if (isFinished()) {
                log.warn("Attempted to get results from finished scan.");
                return ImmutableList.copyOf(new LinkedList<>());
            } else if (!currentResultsEntries.isPresent()) {
                log.warn("Attempted to get results from unstarted scan");
                return ImmutableList.copyOf(new LinkedList<>());
            } else {
                return currentResultsEntries.get();
            }
        }

        /**
         * Get the values read from the previous scan.
         * @return List of Values from the previous scan.
         */
        public List<V> getCurrentResultsValues() {
            if (isFinished()) {
                log.warn("Attempted to get results from finished scan.");
                return ImmutableList.copyOf(new LinkedList<>());
            } else if (currentResultsValues.isPresent()) {
                return currentResultsValues.get();
            } else {
                log.warn("Attempted to get results from unstarted scan");
                return ImmutableList.copyOf(new LinkedList<>());
            }
        }

        /**
         * Perform a scan.
         * @return A scanner object containing the results of the current scan.
         */
        public Scanner getNextResults() {
            if (finished) {
                log.error("Attempted to scan on a finished scanner instance");
                throw new IllegalStateException("Cannot get more results from finished scanner.");
            }
            List<RemoteCorfuTable.TableEntry<K,V>> scannedEntries;
            if (cursor.isPresent()) {
                scannedEntries = adapter.scan(cursor.get(), timestamp);
            } else {
                scannedEntries = adapter.scan(timestamp);
            }
            return handleResults(scannedEntries);
        }

        /**
         * Perform a scan requesting the specified amount of results.
         * @param numResults The desired amount of results from the scan.
         * @return A scanner object containing the results of the current scan.
         */
        public Scanner getNextResults(int numResults) {
            if (finished) {
                log.error("Attempted to scan on a finished scanner instance");
                throw new IllegalStateException("Cannot get more results from finished scanner.");
            }
            List<RemoteCorfuTable.TableEntry<K,V>> scannedEntries;
            if (cursor.isPresent()) {
                scannedEntries = adapter.scan(cursor.get(), numResults, timestamp);
            } else {
                scannedEntries = adapter.scan(numResults, timestamp);
            }
            return handleResults(scannedEntries);
        }

        protected Scanner handleResults(@NonNull List<RemoteCorfuTable.TableEntry<K, V>> scannedEntries) {
            if (scannedEntries.isEmpty()) {
                return new Scanner(true, Optional.empty(), Optional.empty(), Optional.empty(), timestamp);
            } else {
                Optional<ImmutableList<TableEntry<K,V>>> wrappedEntries = Optional.of(ImmutableList.copyOf(scannedEntries));
                Optional<ImmutableList<V>> wrappedValues = Optional.of(getValuesFromEntries(scannedEntries));
                Optional<K> wrappedCursor = Optional.of(scannedEntries.get(scannedEntries.size() - 1).getKey());
                return new Scanner(false, wrappedEntries, wrappedValues, wrappedCursor, timestamp);
            }
        }

        protected ImmutableList<V> getValuesFromEntries(@NonNull List<RemoteCorfuTable.TableEntry<K, V>> scannedEntries) {
            return scannedEntries.stream().map(TableEntry::getValue).collect(ImmutableList.toImmutableList());
        }
    }

    public class ValueFilteringScanner extends Scanner {
        private final Predicate<? super V> valuePredicate;
        private ValueFilteringScanner(boolean finished, Optional<ImmutableList<TableEntry<K, V>>> currentResultsEntries,
                                      Optional<ImmutableList<V>> currentResultsValues, Optional<K> cursor,
                                      long timestamp, Predicate<? super V> valuePredicate) {
            super(finished, currentResultsEntries, currentResultsValues, cursor, timestamp);
            this.valuePredicate = valuePredicate;
        }

        @Override
        protected Scanner handleResults(@NonNull List<TableEntry<K, V>> scannedEntries) {
            if (scannedEntries.isEmpty()) {
                return new ValueFilteringScanner(true,
                        Optional.empty(), Optional.empty(), Optional.empty(), timestamp, valuePredicate);
            } else {
                ImmutableList<TableEntry<K,V>> filteredEntries = filter(scannedEntries);
                Optional<ImmutableList<TableEntry<K,V>>> wrappedEntries = Optional.of(filteredEntries);
                Optional<ImmutableList<V>> wrappedValues = Optional.of(getValuesFromEntries(filteredEntries));
                Optional<K> wrappedCursor = Optional.of(scannedEntries.get(scannedEntries.size() - 1).getKey());
                return new ValueFilteringScanner(false, wrappedEntries, wrappedValues, wrappedCursor, timestamp,
                        valuePredicate);
            }
        }

        private ImmutableList<TableEntry<K,V>> filter(List<TableEntry<K,V>> scannedEntries) {
            return scannedEntries.stream().filter(entry -> valuePredicate.test(entry.getValue()))
                    .collect(ImmutableList.toImmutableList());
        }
    }

    public class EntryFilteringScanner extends Scanner {
        private final Predicate<? super Entry<K, V>> entryPredicate;
        private EntryFilteringScanner(boolean finished, Optional<ImmutableList<TableEntry<K, V>>> currentResultsEntries,
                                      Optional<ImmutableList<V>> currentResultsValues, Optional<K> cursor,
                                      long timestamp, Predicate<? super Entry<K, V>> entryPredicate) {
            super(finished, currentResultsEntries, currentResultsValues, cursor, timestamp);
            this.entryPredicate = entryPredicate;
        }

        @Override
        protected Scanner handleResults(@NonNull List<TableEntry<K, V>> scannedEntries) {
            if (scannedEntries.isEmpty()) {
                return new EntryFilteringScanner(true,
                        Optional.empty(), Optional.empty(), Optional.empty(), timestamp, entryPredicate);
            } else {
                ImmutableList<TableEntry<K,V>> filteredEntries = filter(scannedEntries);
                Optional<ImmutableList<TableEntry<K,V>>> wrappedEntries = Optional.of(filteredEntries);
                Optional<ImmutableList<V>> wrappedValues = Optional.of(getValuesFromEntries(filteredEntries));
                Optional<K> wrappedCursor = Optional.of(scannedEntries.get(scannedEntries.size() - 1).getKey());
                return new EntryFilteringScanner(false, wrappedEntries, wrappedValues, wrappedCursor, timestamp,
                        entryPredicate);
            }
        }

        private ImmutableList<TableEntry<K,V>> filter(List<TableEntry<K,V>> scannedEntries) {
            return scannedEntries.stream().filter(entryPredicate)
                    .collect(ImmutableList.toImmutableList());
        }
    }

    /*----------------------------------------End Scanners------------------------------------------------*/

    /**
     * Factory class to create instances of RemoteCorfuTables.
     */
    public static class RemoteCorfuTableFactory {
        private RemoteCorfuTableFactory() {}

        public static <I,J> RemoteCorfuTable<I,J> openTable(@NonNull CorfuRuntime runtime, @NonNull String tableName) {
            UUID streamID = UUID.nameUUIDFromBytes(tableName.getBytes(StandardCharsets.UTF_8));
            ISerializer serializer = Serializers.getDefaultSerializer();
            IStreamView streamView = new AddressMapStreamView(runtime, streamID);
            RemoteCorfuTableAdapter<I,J> adapter = new RemoteCorfuTableAdapter<>(tableName, streamID, runtime,
                    serializer, streamView);
            UUID checkpointStreamId = getCheckpointStreamIdFromId(streamID);




            try (CorfuTable<UUID, UUID> globalRCTRegistry = runtime.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<UUID, UUID>>() {})
                    .setStreamName("remotecorfutable.globalrctregistry")
                    .open()) {
                globalRCTRegistry.putIfAbsent(streamID, checkpointStreamId);
            }



            return new RemoteCorfuTable<>(adapter, tableName, streamID);
        }
    }
}
