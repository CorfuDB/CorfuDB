package org.corfudb.runtime.collections;

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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

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
                                 Collection<RemoteCorfuTable.RemoteCorfuTableEntry<K,V>> entries) throws RocksDBException {
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
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> entries = new LinkedList<>();
        for (int i = 0; i < 5; i++) {
            RemoteCorfuTable.RemoteCorfuTableEntry<String, String> entry = new RemoteCorfuTable.RemoteCorfuTableEntry<>(
                    "TestKey" + i,
                    "TestValue" + i
            );
            entries.add(entry);
        }
        updateAll(dbHandler, table, entries);
        List<String> readBackKeys = entries.stream()
                .map(RemoteCorfuTable.RemoteCorfuTableEntry::getKey)
                .collect(Collectors.toList());
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> readBackEntries = table.multiGet(readBackKeys);
        ImmutableMultiset<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> expectedWriteSet =
                ImmutableMultiset.copyOf(entries);
        ImmutableMultiset<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> readBackSet =
                ImmutableMultiset.copyOf(readBackEntries);
        assertEquals(expectedWriteSet, readBackSet);

        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> evenEntries = new LinkedList<>();
        for (int i = 0; i < 10; i += 2) {
            String key = "TestKey" + i;
            String val;
            if (i < 5) {
                val = "TestValue" + i;
            } else {
                val = null;
            }
            RemoteCorfuTable.RemoteCorfuTableEntry<String, String> entry = new RemoteCorfuTable.RemoteCorfuTableEntry<>(
                    key, val
            );
            evenEntries.add(entry);
        }
        List<String> evenEntryKeys = evenEntries.stream()
                .map(RemoteCorfuTable.RemoteCorfuTableEntry::getKey)
                .collect(Collectors.toList());
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> readEvenEntries = table.multiGet(evenEntryKeys);
        ImmutableMultiset<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> expectedEvenSet =
                ImmutableMultiset.copyOf(evenEntries);
        ImmutableMultiset<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> readEvenSet =
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
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> entries = new LinkedList<>();
        for (int i = 0; i < 5; i++) {
            RemoteCorfuTable.RemoteCorfuTableEntry<String, String> entry = new RemoteCorfuTable.RemoteCorfuTableEntry<>(
                    "TestKey" + i,
                    "TestValue" + i
            );
            entries.add(entry);
        }
        updateAll(dbHandler, table, entries);
        List<String> deletionKeys = entries.subList(0,3).stream()
                .map(RemoteCorfuTable.RemoteCorfuTableEntry::getKey)
                .collect(Collectors.toList());
        multiDelete(dbHandler, table, deletionKeys);
        ImmutableMultiset<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> expectedViewSet =
                Streams.concat(deletionKeys.stream()
                        .map(key -> new RemoteCorfuTable.RemoteCorfuTableEntry<String,String>(key, null)),
                        entries.subList(3,5).stream()
                        ).collect(ImmutableMultiset.toImmutableMultiset());

        List<String> keys = entries.stream()
                .map(RemoteCorfuTable.RemoteCorfuTableEntry::getKey).collect(Collectors.toList());
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> readView = table.multiGet(keys);
        ImmutableMultiset<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> readViewSet =
                ImmutableMultiset.copyOf(readView);
        assertEquals(expectedViewSet, readViewSet);

        deletionKeys = entries.subList(3,5).stream()
                .map(RemoteCorfuTable.RemoteCorfuTableEntry::getKey)
                .collect(Collectors.toList());
        multiDelete(dbHandler, table, deletionKeys);
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> emptyView = table.multiGet(keys);
        for (RemoteCorfuTable.RemoteCorfuTableEntry<String, String> emptyEntry : emptyView) {
            assertNull(emptyEntry.getValue());
        }
    }

    @Test
    public void testClear() throws RocksDBException {
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> entries = new LinkedList<>();
        for (int i = 0; i < 500; i++) {
            RemoteCorfuTable.RemoteCorfuTableEntry<String, String> entry = new RemoteCorfuTable.RemoteCorfuTableEntry<>(
                    "TestKey" + i,
                    "TestValue" + i
            );
            entries.add(entry);
        }
        updateAll(dbHandler, table, entries);
        clear(dbHandler, table);
        List<String> keys = entries.stream()
                .map(RemoteCorfuTable.RemoteCorfuTableEntry::getKey)
                .collect(Collectors.toList());
        ImmutableMultiset<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> expectedEntrySet = keys.stream()
                .map(key -> new RemoteCorfuTable.RemoteCorfuTableEntry<String, String>(key, null))
                .collect(ImmutableMultiset.toImmutableMultiset());
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> readEntries = table.multiGet(keys);
        ImmutableMultiset<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> readSet =
                ImmutableMultiset.copyOf(readEntries);
        assertEquals(expectedEntrySet, readSet);
    }

    @Test
    public void testLargeScaleDelete() throws RocksDBException {
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> entries = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            RemoteCorfuTable.RemoteCorfuTableEntry<String, String> entry = new RemoteCorfuTable.RemoteCorfuTableEntry<>(
                    "TestKey" + i,
                    "TestValue" + i
            );
            entries.add(entry);
        }
        updateAll(dbHandler, table, entries);
        List<String> keysToDelete = new LinkedList<>();
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> expected = new LinkedList<>();
        for (int i = 0; i < 500; i++) {
            if (i % 2 == 0) {
                expected.add(entries.get(i));
            } else {
                RemoteCorfuTable.RemoteCorfuTableEntry<String, String> deletion =
                        new RemoteCorfuTable.RemoteCorfuTableEntry<>(entries.get(i).getKey(), null);
                keysToDelete.add(deletion.getKey());
                expected.add(deletion);
            }
        }
        multiDelete(dbHandler, table, keysToDelete);
        ImmutableMultiset<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> expectedEntrySet =
                ImmutableMultiset.copyOf(expected);
        List<String> keys = entries.stream()
                .map(RemoteCorfuTable.RemoteCorfuTableEntry::getKey)
                .collect(Collectors.toList());
        List<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> readEntries = table.multiGet(keys);
        ImmutableMultiset<RemoteCorfuTable.RemoteCorfuTableEntry<String, String>> readSet =
                ImmutableMultiset.copyOf(readEntries);
        assertEquals(expectedEntrySet, readSet);
    }
}
