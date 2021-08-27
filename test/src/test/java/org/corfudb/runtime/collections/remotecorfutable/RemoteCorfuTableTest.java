package org.corfudb.runtime.collections.remotecorfutable;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Streams;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperation;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperationFactory;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTable;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class RemoteCorfuTableTest extends AbstractViewTest {
    IStreamView tableStream;
    private CorfuRuntime runtime;
    private DatabaseHandler dbHandler;
    private RemoteCorfuTable<String, String> table;

    @Before
    public void setupTable() throws RocksDBException {
        runtime = getDefaultRuntime();
        dbHandler = getLogUnit(0).getDatabaseHandler();
        table = RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime, "test");
        dbHandler.addTable(table.getStreamId());
        tableStream = runtime.getStreamsView().get(table.getStreamId());
    }

    @After
    public void shutdownTable() throws Exception {
        table.close();
        dbHandler.close();
    }

    //wrapper functions to emulate listening to the log and adding data to the database
    private <K,V> void update(DatabaseHandler dbHandler, RemoteCorfuTable<K,V> table, K key, V value) throws RocksDBException {
        table.insert(key, value);
        applyToDatabase(dbHandler, table.getStreamId());
    }

    private <K,V> void updateAll(DatabaseHandler dbHandler, RemoteCorfuTable<K,V> table,
                                 Collection<RemoteCorfuTable.TableEntry<K,V>> entries) throws RocksDBException {
        table.updateAll(entries);
        applyToDatabase(dbHandler, table.getStreamId());
    }

    private <K,V> void putAll(DatabaseHandler dbHandler, RemoteCorfuTable<K,V> table,
                              Map<? extends K, ? extends V> m) throws RocksDBException {
        table.putAll(m);
        applyToDatabase(dbHandler, table.getStreamId());
    }

    private <K,V> void clear(DatabaseHandler dbHandler, RemoteCorfuTable<K,V> table) throws RocksDBException {
        table.clear();
        applyToDatabase(dbHandler, table.getStreamId());
    }

    private <K,V> void delete(DatabaseHandler dbHandler, RemoteCorfuTable<K,V> table, K key) throws RocksDBException {
        table.delete(key);
        applyToDatabase(dbHandler, table.getStreamId());
    }

    private <K,V> void multiDelete(DatabaseHandler dbHandler, RemoteCorfuTable<K,V> table, List<K> keys) throws RocksDBException {
        table.multiDelete(keys);
        applyToDatabase(dbHandler, table.getStreamId());
    }

    private <K,V> V put(DatabaseHandler dbHandler, RemoteCorfuTable<K,V> table, K key, V value) throws RocksDBException {
        V prevVal = table.get(key);
        update(dbHandler, table, key, value);
        return prevVal;
    }

    private <K,V> V remove(DatabaseHandler dbHandler, RemoteCorfuTable<K,V> table, K key) throws RocksDBException {
        V prevVal = table.get(key);
        delete(dbHandler, table, key);
        return prevVal;
    }

    private void applyToDatabase(DatabaseHandler dbHandler, UUID streamId) throws RocksDBException {
        LogData writtenData = (LogData) tableStream.next();
        assertNotNull(writtenData);
        SMROperation operation = SMROperationFactory.getSMROperation(writtenData, streamId);
        operation.applySMRMethod(dbHandler);
    }

    @Test
    public void testGet() throws RocksDBException {
        update(dbHandler, table, "TestKey", "TestValue");
        String readValue = table.get("TestKey");
        assertEquals("TestValue", readValue);
        readValue = table.get("testkey");
        assertNull(readValue);
    }

    @Test
    public void testMultiGet() throws RocksDBException {
        List<RemoteCorfuTable.TableEntry<String, String>> entries = new LinkedList<>();
        for (int i = 0; i < 5; i++) {
            RemoteCorfuTable.TableEntry<String, String> entry = new RemoteCorfuTable.TableEntry<>(
                    "TestKey" + i,
                    "TestValue" + i
            );
            entries.add(entry);
        }
        updateAll(dbHandler, table, entries);
        List<String> readBackKeys = entries.stream()
                .map(RemoteCorfuTable.TableEntry::getKey)
                .collect(Collectors.toList());
        List<RemoteCorfuTable.TableEntry<String, String>> readBackEntries = table.multiGet(readBackKeys);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<String, String>> expectedWriteSet =
                ImmutableMultiset.copyOf(entries);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<String, String>> readBackSet =
                ImmutableMultiset.copyOf(readBackEntries);
        assertEquals(expectedWriteSet, readBackSet);

        List<RemoteCorfuTable.TableEntry<String, String>> evenEntries = new LinkedList<>();
        for (int i = 0; i < 10; i += 2) {
            String key = "TestKey" + i;
            String val;
            if (i < 5) {
                val = "TestValue" + i;
            } else {
                val = null;
            }
            RemoteCorfuTable.TableEntry<String, String> entry = new RemoteCorfuTable.TableEntry<>(
                    key, val
            );
            evenEntries.add(entry);
        }
        List<String> evenEntryKeys = evenEntries.stream()
                .map(RemoteCorfuTable.TableEntry::getKey)
                .collect(Collectors.toList());
        List<RemoteCorfuTable.TableEntry<String, String>> readEvenEntries = table.multiGet(evenEntryKeys);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<String, String>> expectedEvenSet =
                ImmutableMultiset.copyOf(evenEntries);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<String, String>> readEvenSet =
                ImmutableMultiset.copyOf(readEvenEntries);
        assertEquals(expectedEvenSet, readEvenSet);
    }

    @Test
    public void testRemove() throws RocksDBException {
        String key = "TestKey";
        String val = "TestValue";
        update(dbHandler, table, key, val);
        String readVal = table.get(key);
        assertEquals(val, readVal);
        String removedVal = remove(dbHandler, table, key);
        assertEquals(val, removedVal);
        String readingRemoved = table.get(key);
        assertNull(readingRemoved);
        String doubleRemoved = remove(dbHandler, table, key);
        assertNull(doubleRemoved);
    }

    @Test
    public void testPut() throws RocksDBException {
        String key = "TestKey";
        String prevVal = "TestPrev";
        String currVal = "TestCurr";
        String readVal = put(dbHandler, table, key, prevVal);
        assertNull(readVal);
        readVal = table.get(key);
        assertEquals(prevVal, readVal);
        readVal = put(dbHandler, table, key, currVal);
        assertEquals(prevVal, readVal);
        readVal = table.get(key);
        assertEquals(currVal, readVal);
    }

    @Test
    public void testMultiDelete() throws RocksDBException {
        List<RemoteCorfuTable.TableEntry<String, String>> entries = new LinkedList<>();
        for (int i = 0; i < 5; i++) {
            RemoteCorfuTable.TableEntry<String, String> entry = new RemoteCorfuTable.TableEntry<>(
                    "TestKey" + i,
                    "TestValue" + i
            );
            entries.add(entry);
        }
        updateAll(dbHandler, table, entries);
        List<String> deletionKeys = entries.subList(0,3).stream()
                .map(RemoteCorfuTable.TableEntry::getKey)
                .collect(Collectors.toList());
        multiDelete(dbHandler, table, deletionKeys);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<String, String>> expectedViewSet =
                Streams.concat(deletionKeys.stream()
                        .map(key -> new RemoteCorfuTable.TableEntry<String,String>(key, null)),
                        entries.subList(3,5).stream()
                        ).collect(ImmutableMultiset.toImmutableMultiset());

        List<String> keys = entries.stream()
                .map(RemoteCorfuTable.TableEntry::getKey).collect(Collectors.toList());
        List<RemoteCorfuTable.TableEntry<String, String>> readView = table.multiGet(keys);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<String, String>> readViewSet =
                ImmutableMultiset.copyOf(readView);
        assertEquals(expectedViewSet, readViewSet);

        deletionKeys = entries.subList(3,5).stream()
                .map(RemoteCorfuTable.TableEntry::getKey)
                .collect(Collectors.toList());
        multiDelete(dbHandler, table, deletionKeys);
        List<RemoteCorfuTable.TableEntry<String, String>> emptyView = table.multiGet(keys);
        for (RemoteCorfuTable.TableEntry<String, String> emptyEntry : emptyView) {
            assertNull(emptyEntry.getValue());
        }
    }

    @Test
    public void testClear() throws RocksDBException {
        List<RemoteCorfuTable.TableEntry<String, String>> entries = new LinkedList<>();
        for (int i = 0; i < 500; i++) {
            RemoteCorfuTable.TableEntry<String, String> entry = new RemoteCorfuTable.TableEntry<>(
                    "TestKey" + i,
                    "TestValue" + i
            );
            entries.add(entry);
        }
        updateAll(dbHandler, table, entries);
        clear(dbHandler, table);
        List<String> keys = entries.stream()
                .map(RemoteCorfuTable.TableEntry::getKey)
                .collect(Collectors.toList());
        ImmutableMultiset<RemoteCorfuTable.TableEntry<String, String>> expectedEntrySet = keys.stream()
                .map(key -> new RemoteCorfuTable.TableEntry<String, String>(key, null))
                .collect(ImmutableMultiset.toImmutableMultiset());
        List<RemoteCorfuTable.TableEntry<String, String>> readEntries = table.multiGet(keys);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<String, String>> readSet =
                ImmutableMultiset.copyOf(readEntries);
        assertEquals(expectedEntrySet, readSet);
    }

    @Test
    public void testLargeScaleDelete() throws RocksDBException {
        List<RemoteCorfuTable.TableEntry<String, String>> entries = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            RemoteCorfuTable.TableEntry<String, String> entry = new RemoteCorfuTable.TableEntry<>(
                    "TestKey" + i,
                    "TestValue" + i
            );
            entries.add(entry);
        }
        updateAll(dbHandler, table, entries);
        List<String> keysToDelete = new LinkedList<>();
        List<RemoteCorfuTable.TableEntry<String, String>> expected = new LinkedList<>();
        for (int i = 0; i < 500; i++) {
            if (i % 2 == 0) {
                expected.add(entries.get(i));
            } else {
                RemoteCorfuTable.TableEntry<String, String> deletion =
                        new RemoteCorfuTable.TableEntry<>(entries.get(i).getKey(), null);
                keysToDelete.add(deletion.getKey());
                expected.add(deletion);
            }
        }
        multiDelete(dbHandler, table, keysToDelete);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<String, String>> expectedEntrySet =
                ImmutableMultiset.copyOf(expected);
        List<String> keys = entries.stream()
                .map(RemoteCorfuTable.TableEntry::getKey)
                .collect(Collectors.toList());
        List<RemoteCorfuTable.TableEntry<String, String>> readEntries = table.multiGet(keys);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<String, String>> readSet =
                ImmutableMultiset.copyOf(readEntries);
        assertEquals(expectedEntrySet, readSet);
    }

    @Test
    public void testScanDefaultSize() throws RocksDBException {
        //using an int->string table for sorting convenience
        RemoteCorfuTable<Integer, String> intTable =
                RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime, "test2");
        dbHandler.addTable(intTable.getStreamId());
        tableStream = runtime.getStreamsView().get(intTable.getStreamId());

        List<Integer> keys = IntStream.range(0,500).boxed().sorted((a, b) -> b.toString().compareTo(a.toString()))
                .collect(Collectors.toList());
        List<RemoteCorfuTable.TableEntry<Integer, String>> entries = keys.stream()
                .map(i -> new RemoteCorfuTable.TableEntry<>(i, "Val" + i))
                .collect(Collectors.toList());

        updateAll(dbHandler, intTable, entries);

        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> expectedEntries =
                ImmutableMultiset.copyOf(entries);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> readEntries =
                ImmutableMultiset.copyOf(intTable.multiGet(keys));
        assertEquals(expectedEntries, readEntries);

        RemoteCorfuTable<Integer, String>.Scanner scanner = intTable.getScanner();
        scanner = scanner.getNextResults();
        List<RemoteCorfuTable.TableEntry<Integer, String>> scannedEntries = scanner.getCurrentResultsEntries();
        int startPos = 0;
        int endPos = scannedEntries.size();
        assertEquals(entries.subList(startPos, endPos), scannedEntries);
        startPos = endPos;
        while (!scanner.isFinished()) {
            scanner = scanner.getNextResults();
            scannedEntries = scanner.getCurrentResultsEntries();
            endPos += scannedEntries.size();
            assertEquals(entries.subList(startPos, endPos), scannedEntries);
            startPos = endPos;
        }
    }

    @Test
    public void testScanVariableSize() throws RocksDBException {
        //using an int->string table for sorting convenience
        RemoteCorfuTable<Integer, String> intTable =
                RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime, "test2");
        dbHandler.addTable(intTable.getStreamId());
        tableStream = runtime.getStreamsView().get(intTable.getStreamId());

        List<Integer> keys = IntStream.range(0,500).boxed().sorted((a, b) -> b.toString().compareTo(a.toString()))
                .collect(Collectors.toList());
        List<RemoteCorfuTable.TableEntry<Integer, String>> entries = keys.stream()
                .map(i -> new RemoteCorfuTable.TableEntry<>(i, "Val" + i))
                .collect(Collectors.toList());

        updateAll(dbHandler, intTable, entries);

        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> expectedEntries =
                ImmutableMultiset.copyOf(entries);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> readEntries =
                ImmutableMultiset.copyOf(intTable.multiGet(keys));
        assertEquals(expectedEntries, readEntries);

        int scanSize = 1;

        RemoteCorfuTable<Integer, String>.Scanner scanner = intTable.getScanner();
        scanner = scanner.getNextResults(scanSize);
        List<RemoteCorfuTable.TableEntry<Integer, String>> scannedEntries = scanner.getCurrentResultsEntries();
        scanSize += 2;
        int startPos = 0;
        int endPos = scannedEntries.size();
        assertEquals(entries.subList(startPos, endPos), scannedEntries);
        startPos = endPos;
        while (!scanner.isFinished()) {
            scanner = scanner.getNextResults(scanSize);
            scannedEntries = scanner.getCurrentResultsEntries();
            scanSize += 2;
            endPos += scannedEntries.size();
            assertEquals(entries.subList(startPos, endPos), scannedEntries);
            startPos = endPos;
        }
    }

    @Test
    public void testFixedSizeEntryFilterScan() throws RocksDBException {
        //using an int->string table for sorting convenience
        RemoteCorfuTable<Integer, String> intTable =
                RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime, "test2");
        dbHandler.addTable(intTable.getStreamId());
        tableStream = runtime.getStreamsView().get(intTable.getStreamId());

        List<Integer> keys = IntStream.range(0,500).boxed().sorted((a, b) -> b.toString().compareTo(a.toString()))
                .collect(Collectors.toList());
        List<RemoteCorfuTable.TableEntry<Integer, String>> entries = keys.stream()
                .map(i -> new RemoteCorfuTable.TableEntry<>(i, "Val" + i))
                .collect(Collectors.toList());

        updateAll(dbHandler, intTable, entries);

        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> expectedEntries =
                ImmutableMultiset.copyOf(entries);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> readEntries =
                ImmutableMultiset.copyOf(intTable.multiGet(keys));
        assertEquals(expectedEntries, readEntries);
        final Predicate<Map.Entry<Integer, String>> entryPredicate = entry -> entry.getKey() % 9 == 0;
        RemoteCorfuTable<Integer, String>.Scanner scanner = intTable.getEntryFilterScanner(entryPredicate);
        scanner = scanner.getNextResults();
        List<RemoteCorfuTable.TableEntry<Integer, String>> scannedEntries = scanner.getCurrentResultsEntries();
        int startPos = 0;
        int endPos = 20;
        List<RemoteCorfuTable.TableEntry<Integer, String>> expectedFilteredEntries
                = filterSublistByEntry(entries, entryPredicate, startPos, endPos);
        assertEquals(expectedFilteredEntries, scannedEntries);
        startPos = endPos;
        while (!scanner.isFinished()) {
            scanner = scanner.getNextResults();
            scannedEntries = scanner.getCurrentResultsEntries();
            endPos += 20;
            if (endPos > entries.size()) {
                endPos = entries.size();
            }
            expectedFilteredEntries = filterSublistByEntry(entries, entryPredicate, startPos, endPos);
            assertEquals(expectedFilteredEntries, scannedEntries);
            startPos = endPos;
        }
    }

    @Test
    public void testEntryFilterScanVariableSize() throws RocksDBException {
        //using an int->string table for sorting convenience
        RemoteCorfuTable<Integer, String> intTable =
                RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime, "test2");
        dbHandler.addTable(intTable.getStreamId());
        tableStream = runtime.getStreamsView().get(intTable.getStreamId());

        List<Integer> keys = IntStream.range(0,500).boxed().sorted((a, b) -> b.toString().compareTo(a.toString()))
                .collect(Collectors.toList());
        List<RemoteCorfuTable.TableEntry<Integer, String>> entries = keys.stream()
                .map(i -> new RemoteCorfuTable.TableEntry<>(i, "Val" + i))
                .collect(Collectors.toList());

        updateAll(dbHandler, intTable, entries);

        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> expectedEntries =
                ImmutableMultiset.copyOf(entries);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> readEntries =
                ImmutableMultiset.copyOf(intTable.multiGet(keys));
        assertEquals(expectedEntries, readEntries);

        int scanSize = 1;

        final Predicate<Map.Entry<Integer, String>> entryPredicate = entry -> entry.getKey() % 9 == 0;
        RemoteCorfuTable<Integer, String>.Scanner scanner = intTable.getEntryFilterScanner(entryPredicate);
        scanner = scanner.getNextResults(scanSize);
        List<RemoteCorfuTable.TableEntry<Integer, String>> scannedEntries = scanner.getCurrentResultsEntries();
        int startPos = 0;
        int endPos = 1;
        scanSize += 2;
        List<RemoteCorfuTable.TableEntry<Integer, String>> expectedFilteredEntries
                = filterSublistByEntry(entries, entryPredicate, startPos, endPos);
        assertEquals(expectedFilteredEntries, scannedEntries);
        startPos = endPos;
        while (!scanner.isFinished()) {
            scanner = scanner.getNextResults(scanSize);
            scannedEntries = scanner.getCurrentResultsEntries();
            endPos += scanSize;
            if (endPos > entries.size()) {
                endPos = entries.size();
            }
            scanSize += 2;
            expectedFilteredEntries
                    = filterSublistByEntry(entries, entryPredicate, startPos, endPos);
            assertEquals(expectedFilteredEntries, scannedEntries);
            startPos = endPos;
        }
    }

    private List<RemoteCorfuTable.TableEntry<Integer, String>> filterSublistByEntry(
            List<RemoteCorfuTable.TableEntry<Integer, String>> entries,
            Predicate<Map.Entry<Integer, String>> entryPredicate, int startPos, int endPos) {
        return entries.subList(startPos, endPos).stream().filter(entryPredicate).collect(Collectors.toList());
    }

    @Test
    public void testFixedSizeValueFilterScan() throws RocksDBException {
        //using an int->string table for sorting convenience
        RemoteCorfuTable<Integer, String> intTable =
                RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime, "test2");
        dbHandler.addTable(intTable.getStreamId());
        tableStream = runtime.getStreamsView().get(intTable.getStreamId());

        List<Integer> keys = IntStream.range(0,500).boxed().sorted((a, b) -> b.toString().compareTo(a.toString()))
                .collect(Collectors.toList());
        List<RemoteCorfuTable.TableEntry<Integer, String>> entries = keys.stream()
                .map(i -> new RemoteCorfuTable.TableEntry<>(i, "Val" + i))
                .collect(Collectors.toList());

        updateAll(dbHandler, intTable, entries);

        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> expectedEntries =
                ImmutableMultiset.copyOf(entries);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> readEntries =
                ImmutableMultiset.copyOf(intTable.multiGet(keys));
        assertEquals(expectedEntries, readEntries);
        final Predicate<String> valuePredicate = val -> val.endsWith("9");
        RemoteCorfuTable<Integer, String>.Scanner scanner = intTable.getValueFilterScanner(valuePredicate);
        scanner = scanner.getNextResults();
        List<RemoteCorfuTable.TableEntry<Integer, String>> scannedEntries = scanner.getCurrentResultsEntries();
        int startPos = 0;
        int endPos = 20;
        List<RemoteCorfuTable.TableEntry<Integer, String>> expectedFilteredEntries
                = filterSublistByValue(entries, valuePredicate, startPos, endPos);
        assertEquals(expectedFilteredEntries, scannedEntries);
        List<String> expectedFilteredValues = getValuesFromList(expectedFilteredEntries);
        List<String> scannedValues = scanner.getCurrentResultsValues();
        assertEquals(expectedFilteredValues, scannedValues);
        startPos = endPos;
        while (!scanner.isFinished()) {
            scanner = scanner.getNextResults();
            scannedEntries = scanner.getCurrentResultsEntries();
            endPos += 20;
            if (endPos > entries.size()) {
                endPos = entries.size();
            }
            expectedFilteredEntries = filterSublistByValue(entries, valuePredicate, startPos, endPos);
            assertEquals(expectedFilteredEntries, scannedEntries);
            expectedFilteredValues = getValuesFromList(expectedFilteredEntries);
            scannedValues = scanner.getCurrentResultsValues();
            assertEquals(expectedFilteredValues, scannedValues);
            startPos = endPos;
        }
    }

    @Test
    public void testValueFilterScanVariableSize() throws RocksDBException {
        //using an int->string table for sorting convenience
        RemoteCorfuTable<Integer, String> intTable =
                RemoteCorfuTable.RemoteCorfuTableFactory.openTable(runtime, "test2");
        dbHandler.addTable(intTable.getStreamId());
        tableStream = runtime.getStreamsView().get(intTable.getStreamId());

        List<Integer> keys = IntStream.range(0,500).boxed().sorted((a, b) -> b.toString().compareTo(a.toString()))
                .collect(Collectors.toList());
        List<RemoteCorfuTable.TableEntry<Integer, String>> entries = keys.stream()
                .map(i -> new RemoteCorfuTable.TableEntry<>(i, "Val" + i))
                .collect(Collectors.toList());

        updateAll(dbHandler, intTable, entries);

        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> expectedEntries =
                ImmutableMultiset.copyOf(entries);
        ImmutableMultiset<RemoteCorfuTable.TableEntry<Integer, String>> readEntries =
                ImmutableMultiset.copyOf(intTable.multiGet(keys));
        assertEquals(expectedEntries, readEntries);

        int scanSize = 1;

        final Predicate<String> valuePredicate = val -> val.endsWith("9");
        RemoteCorfuTable<Integer, String>.Scanner scanner = intTable.getValueFilterScanner(valuePredicate);
        scanner = scanner.getNextResults(scanSize);
        List<RemoteCorfuTable.TableEntry<Integer, String>> scannedEntries = scanner.getCurrentResultsEntries();
        int startPos = 0;
        int endPos = 1;
        scanSize += 2;
        List<RemoteCorfuTable.TableEntry<Integer, String>> expectedFilteredEntries
                = filterSublistByValue(entries, valuePredicate, startPos, endPos);
        assertEquals(expectedFilteredEntries, scannedEntries);
        List<String> expectedFilteredValues = getValuesFromList(expectedFilteredEntries);
        List<String> scannedValues = scanner.getCurrentResultsValues();
        assertEquals(expectedFilteredValues, scannedValues);
        startPos = endPos;
        while (!scanner.isFinished()) {
            scanner = scanner.getNextResults(scanSize);
            scannedEntries = scanner.getCurrentResultsEntries();
            endPos += scanSize;
            if (endPos > entries.size()) {
                endPos = entries.size();
            }
            scanSize += 2;
            expectedFilteredEntries = filterSublistByValue(entries, valuePredicate, startPos, endPos);
            assertEquals(expectedFilteredEntries, scannedEntries);
            expectedFilteredValues = getValuesFromList(expectedFilteredEntries);
            scannedValues = scanner.getCurrentResultsValues();
            assertEquals(expectedFilteredValues, scannedValues);
            startPos = endPos;
        }
    }

    private List<RemoteCorfuTable.TableEntry<Integer, String>> filterSublistByValue(
            List<RemoteCorfuTable.TableEntry<Integer, String>> entries,
            Predicate<String> valuePredicate, int startPos, int endPos) {
        return entries.subList(startPos, endPos).stream().filter(entry -> valuePredicate.test(entry.getValue()))
                .collect(Collectors.toList());
    }

    private List<String> getValuesFromList(
            List<RemoteCorfuTable.TableEntry<Integer, String>> entries) {
        return entries.stream().map(RemoteCorfuTable.TableEntry::getValue)
                .collect(Collectors.toList());
    }

    @Test
    public void testSize() throws RocksDBException {
        assertTrue(table.isEmpty());
        List<RemoteCorfuTable.TableEntry<String, String>> entries;
        for (int start = 0, increment = 1, end = 1; end < 1000; start = end, increment++, end = start + increment) {
            entries = IntStream.range(start, end).mapToObj(i -> new RemoteCorfuTable.TableEntry<>(
                    "Key" + i,
                    "Val" + i
            )).collect(Collectors.toList());
            updateAll(dbHandler, table, entries);
            int readSize = table.size();
            assertEquals(end, readSize);
        }
    }

    @Test
    public void testContainsKey() throws RocksDBException {
        List<RemoteCorfuTable.TableEntry<String, String>> entries = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            RemoteCorfuTable.TableEntry<String, String> entry = new RemoteCorfuTable.TableEntry<>(
                    "TestKey" + i,
                    "TestValue" + i
            );
            entries.add(entry);
        }
        updateAll(dbHandler, table, entries);
        List<String> deletionList = IntStream.range(0, 500).filter(i -> i % 2 == 0)
                .mapToObj(entries::get).map(RemoteCorfuTable.TableEntry::getKey).collect(Collectors.toList());
        multiDelete(dbHandler, table, deletionList);
        Set<String> deletedSet = new HashSet<>(deletionList);
        for (RemoteCorfuTable.TableEntry<String, String> entry : entries) {
            String key = entry.getKey();
            boolean deleted = deletedSet.contains(key);
            boolean contained = table.containsKey(key);
            System.out.printf("Testing pair %s -> %s\n", entry.getKey(), entry.getValue());
            assertNotEquals(deleted, contained);
        }
    }

    @Test
    public void testContainsValue() throws RocksDBException {
        List<RemoteCorfuTable.TableEntry<String, String>> entries = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            RemoteCorfuTable.TableEntry<String, String> entry = new RemoteCorfuTable.TableEntry<>(
                    "TestKey" + i,
                    "TestValue" + i
            );
            entries.add(entry);
        }
        updateAll(dbHandler, table, entries);
        List<String> deletionList = IntStream.range(0, 500).filter(i -> i % 2 == 0)
                .mapToObj(entries::get).map(RemoteCorfuTable.TableEntry::getKey).collect(Collectors.toList());
        multiDelete(dbHandler, table, deletionList);
        Set<String> deletedSet = new HashSet<>(deletionList);
        for (RemoteCorfuTable.TableEntry<String, String> entry : entries) {
            boolean deleted = deletedSet.contains(entry.getKey());
            boolean contained = table.containsValue(entry.getValue());
            assertNotEquals(deleted, contained);
        }
    }
}
