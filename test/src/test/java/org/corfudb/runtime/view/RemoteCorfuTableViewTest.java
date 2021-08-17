package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.ByteString;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class RemoteCorfuTableViewTest extends AbstractViewTest {

    @Test
    public void canReadSingleEntry() throws RocksDBException {
        CorfuRuntime r = getDefaultRuntime();
        DatabaseHandler dbHandler = getLogUnit(0).getDatabaseHandler();
        RemoteCorfuTableVersionedKey dummyKey = new RemoteCorfuTableVersionedKey(
                ByteString.copyFrom("dummyKey", StandardCharsets.UTF_8), 0);
        ByteString dummyValue = ByteString.copyFrom("dummyValue", StandardCharsets.UTF_8);
        CorfuTable<Integer, Integer> dummyTable = r.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {})
                .setStreamName("dummy")
                .open();
        dummyTable.put(1,2);
        UUID tableUUID = dummyTable.getCorfuStreamID();
        dbHandler.addTable(tableUUID);
        dbHandler.update(dummyKey, dummyValue, tableUUID);
        ByteString readValue = r.getRemoteCorfuTableView().get(dummyKey, tableUUID);
        assertEquals(dummyValue, readValue);
    }

    @Test
    public void canReadMultipleEntries() throws RocksDBException {
        CorfuRuntime r = getDefaultRuntime();
        DatabaseHandler dbHandler = getLogUnit(0).getDatabaseHandler();
        CorfuTable<Integer, Integer> dummyTable = r.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {})
                .setStreamName("dummy")
                .open();
        dummyTable.put(1,2);
        UUID tableUUID = dummyTable.getCorfuStreamID();
        long timestamp = r.getSequencerView().query(tableUUID);
        List<RemoteCorfuTableEntry> entries = new ArrayList<>(5);
        List<RemoteCorfuTableEntry> evenEntries = new ArrayList<>(3);
        for (int i = 0; i < 5; i++) {
            RemoteCorfuTableEntry newEntry = new RemoteCorfuTableEntry(
                    new RemoteCorfuTableVersionedKey(ByteString.copyFrom("key" + i, StandardCharsets.UTF_8),
                            timestamp),
                    ByteString.copyFrom("val" + i, StandardCharsets.UTF_8));
            entries.add(newEntry);
            if (i % 2 == 0) {
                evenEntries.add(newEntry);
            }
        }
        dbHandler.addTable(tableUUID);
        dbHandler.updateAll(entries, tableUUID);
        List<RemoteCorfuTableVersionedKey> evenKeys = evenEntries.stream().map(RemoteCorfuTableEntry::getKey)
                .collect(Collectors.toList());
        List<RemoteCorfuTableEntry> readEntries = r.getRemoteCorfuTableView().multiGet(evenKeys, tableUUID);
        ImmutableMultiset<RemoteCorfuTableEntry> expectedSet = ImmutableMultiset.copyOf(evenEntries);
        ImmutableMultiset<RemoteCorfuTableEntry> readSet = ImmutableMultiset.copyOf(readEntries);
        assertEquals(expectedSet, readSet);
    }

    @Test
    public void canScanTable() throws RocksDBException {
        CorfuRuntime r = getDefaultRuntime();
        DatabaseHandler dbHandler = getLogUnit(0).getDatabaseHandler();
        List<RemoteCorfuTableEntry> entries = new ArrayList<>(5);
        CorfuTable<Integer, Integer> dummyTable = r.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {})
                .setStreamName("dummy")
                .open();
        dummyTable.put(1,2);
        UUID tableUUID = dummyTable.getCorfuStreamID();
        long timestamp = r.getSequencerView().query(tableUUID);
        for (int i = 0; i < 5; i++) {
            entries.add(new RemoteCorfuTableEntry(
                    new RemoteCorfuTableVersionedKey(ByteString.copyFrom("key" + i, StandardCharsets.UTF_8),
                            timestamp),
                    ByteString.copyFrom("val"+i, StandardCharsets.UTF_8)));
        }

        dbHandler.addTable(tableUUID);
        dbHandler.updateAll(entries, tableUUID);
        List<RemoteCorfuTableEntry> readEntries = r.getRemoteCorfuTableView().scan(tableUUID, timestamp);
        assertEquals(entries.size(), readEntries.size());
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(entries.get(i), readEntries.get(4-i));
        }
    }

    @Test
    public void canContainsKey() throws RocksDBException {
        CorfuRuntime r = getDefaultRuntime();
        DatabaseHandler dbHandler = getLogUnit(0).getDatabaseHandler();
        List<RemoteCorfuTableEntry> entries = new ArrayList<>(10);
        CorfuTable<Integer, Integer> dummyTable = r.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {})
                .setStreamName("dummy")
                .open();
        dummyTable.put(1,2);
        UUID tableUUID = dummyTable.getCorfuStreamID();
        long timestamp = r.getSequencerView().query(tableUUID);
        List<RemoteCorfuTableEntry> writeSet = new ArrayList<>(5);
        for (int i = 0; i < 10; i++) {
            RemoteCorfuTableEntry newEntry = new RemoteCorfuTableEntry(
                    new RemoteCorfuTableVersionedKey(ByteString.copyFrom("key" + i, StandardCharsets.UTF_8),
                            timestamp),
                    ByteString.copyFrom("val"+i, StandardCharsets.UTF_8));
            entries.add(newEntry);
            if (i % 2 == 0) {
                writeSet.add(newEntry);
            }
        }
        dbHandler.addTable(tableUUID);
        dbHandler.updateAll(writeSet, tableUUID);
        boolean contains;
        for (int i = 0; i < 10; i++) {
            contains = r.getRemoteCorfuTableView().containsKey(entries.get(i).getKey(), tableUUID);
            if (i % 2 != 0) {
                assertFalse(contains);
            } else {
                assertTrue(contains);
            }
        }
    }

    @Test
    public void canContainsValue() throws RocksDBException {
        CorfuRuntime r = getDefaultRuntime();
        DatabaseHandler dbHandler = getLogUnit(0).getDatabaseHandler();
        List<RemoteCorfuTableEntry> entries = new ArrayList<>(10);
        CorfuTable<Integer, Integer> dummyTable = r.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {})
                .setStreamName("dummy")
                .open();
        dummyTable.put(1,2);
        UUID tableUUID = dummyTable.getCorfuStreamID();
        long timestamp = r.getSequencerView().query(tableUUID);
        List<RemoteCorfuTableEntry> writeSet = new ArrayList<>(5);
        for (int i = 0; i < 10; i++) {
            RemoteCorfuTableEntry newEntry = new RemoteCorfuTableEntry(
                    new RemoteCorfuTableVersionedKey(ByteString.copyFrom("key" + i, StandardCharsets.UTF_8),
                            timestamp),
                    ByteString.copyFrom("val"+i, StandardCharsets.UTF_8));
            entries.add(newEntry);
            if (i % 2 == 0) {
                writeSet.add(newEntry);
            }
        }
        dbHandler.addTable(tableUUID);
        dbHandler.updateAll(writeSet, tableUUID);
        boolean contains;
        for (int i = 0; i < 10; i++) {
            contains = r.getRemoteCorfuTableView().containsValue(
                    entries.get(i).getValue(), tableUUID, timestamp, 10);
            if (i % 2 != 0) {
                assertFalse(contains);
            } else {
                assertTrue(contains);
            }
        }
    }

    @Test
    public void canSizeTable() throws RocksDBException {
        CorfuRuntime r = getDefaultRuntime();
        DatabaseHandler dbHandler = getLogUnit(0).getDatabaseHandler();
        CorfuTable<Integer, Integer> dummyTable = r.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {})
                .setStreamName("dummy")
                .open();
        dummyTable.put(1,2);
        UUID tableUUID = dummyTable.getCorfuStreamID();
        long timestamp = r.getSequencerView().query(tableUUID);
        List<RemoteCorfuTableEntry> writeSet = new ArrayList<>(50);
        for (int i = 0; i < 50; i++) {
            RemoteCorfuTableEntry newEntry = new RemoteCorfuTableEntry(
                    new RemoteCorfuTableVersionedKey(ByteString.copyFrom("key" + i, StandardCharsets.UTF_8),
                            timestamp),
                    ByteString.copyFrom("val"+i, StandardCharsets.UTF_8));
            writeSet.add(newEntry);
        }
        dbHandler.addTable(tableUUID);
        dbHandler.updateAll(writeSet, tableUUID);
        assertEquals(writeSet.size(), r.getRemoteCorfuTableView().size(tableUUID, timestamp, 50));
    }
}
