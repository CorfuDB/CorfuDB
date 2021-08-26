package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import static org.corfudb.common.remotecorfutable.DatabaseConstants.DATABASE_CHARSET;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The DatabaseHandlerTest provides Unit tests for the DatabaseHandler object.
 *
 * <p>Created by nvaishampayan517 on 7/27/21.
 */
@Slf4j
public class DatabaseHandlerTest {
    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    //Object to test
    private DatabaseHandler databaseHandler;

    //Objects to mock
    private Path mPath;
    private File mFile;
    private Options mOptions;
    private ExecutorService mExecutor;

    //constants
    private final UUID stream1 = UUID.nameUUIDFromBytes("stream1".getBytes(DATABASE_CHARSET));
    private final ByteString key1 = ByteString.copyFrom("key1",DATABASE_CHARSET);

    /**
     * Mocks required objects and sets up database handler.
     */
    @Before
    public void setup() {
        mPath = mock(Path.class);
        mOptions = DatabaseHandler.getDefaultOptions();
        mExecutor = mock(ExecutorService.class);
        mFile = mock(File.class);

        String TEST_TEMP_DIR = com.google.common.io.Files.createTempDir().getAbsolutePath();

        when(mFile.getAbsolutePath()).thenReturn(TEST_TEMP_DIR);
        when(mPath.toFile()).thenReturn(mFile);

        databaseHandler = new DatabaseHandler(mPath,mOptions,mExecutor, 5);
    }

    @After
    public void dispose() {
        databaseHandler.close();
    }

    @Test
    public void testPutandGetBasicFunctionality() throws RocksDBException, DatabaseOperationException {
        ByteString expectedValue = ByteString.copyFrom("ver0val", DATABASE_CHARSET);
        RemoteCorfuTableVersionedKey encodedKey = new RemoteCorfuTableVersionedKey(key1,0L);
        ByteString readinVal;
        try {
            databaseHandler.addTable(stream1);
            databaseHandler.update(encodedKey,expectedValue,stream1);
            readinVal = databaseHandler.get(encodedKey,stream1);
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test PUT and GET Basic: ", e);
            throw e;
        }
        assertEquals(expectedValue, readinVal);
    }

    @Test
    public void testVersionedGetFunctionality() throws RocksDBException, DatabaseOperationException {
        ByteString v1Val = ByteString.copyFrom("ver1val".getBytes(DATABASE_CHARSET));
        ByteString v2Val = ByteString.copyFrom("ver2val".getBytes(DATABASE_CHARSET));
        ByteString v4Val = ByteString.copyFrom("ver1val".getBytes(DATABASE_CHARSET));
        RemoteCorfuTableVersionedKey[] keys = new RemoteCorfuTableVersionedKey[6];
        for (int i = 0; i < 6; i++) {
            keys[i] = new RemoteCorfuTableVersionedKey(key1,i);
        }
        ByteString readinVal;
        try {
            databaseHandler.addTable(stream1);
            databaseHandler.update(keys[1],v1Val,stream1);
            databaseHandler.update(keys[2],v2Val,stream1);
            databaseHandler.update(keys[4],v4Val,stream1);
            for (int i = 0; i < 6; i++) {
                readinVal = databaseHandler.get(keys[i],stream1);
                switch (i) {
                    case 5: case 4:
                        assertEquals(v4Val, readinVal);
                        break;
                    case 3: case 2:
                        assertEquals(v2Val, readinVal);
                        break;
                    case 1:
                        assertEquals(v1Val, readinVal);
                        break;
                    default:
                        assertTrue(readinVal.isEmpty());
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test versioned GET: ", e);
            throw e;
        }
    }

    @Test
    public void testUpdateAllBasic() throws RocksDBException {
        List<RemoteCorfuTableDatabaseEntry> keyValuePairs = new LinkedList<>();
        for (int i = 0; i < 1000; i++) {
            byte[][] pair = new byte[2][];
            RemoteCorfuTableVersionedKey pairKey = new RemoteCorfuTableVersionedKey(
                    ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET), Longs.toByteArray(i))),
                    0L);
            ByteString pairValue = ByteString.copyFrom(("val" + i).getBytes(DATABASE_CHARSET));
            keyValuePairs.add(new RemoteCorfuTableDatabaseEntry(pairKey, pairValue));
        }
        databaseHandler.addTable(stream1);
        databaseHandler.updateAll(keyValuePairs, stream1);
        List<RemoteCorfuTableDatabaseEntry> read = databaseHandler.fullDatabaseScan(stream1);
        for (int i = 0; i < 1000; i++) {
            assertEquals(keyValuePairs.get(i),read.get(999-i));
        }
    }

    @Test
    public void testMultiGetBasic() throws RocksDBException {
        List<RemoteCorfuTableDatabaseEntry> keyValuePairs = new LinkedList<>();
        for (int i = 0; i < 1000; i++) {
            byte[][] pair = new byte[2][];
            RemoteCorfuTableVersionedKey pairKey = new RemoteCorfuTableVersionedKey(
                    ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET), Longs.toByteArray(i))),
                    0L);
            ByteString pairValue = ByteString.copyFrom(("val" + i).getBytes(DATABASE_CHARSET));
            keyValuePairs.add(new RemoteCorfuTableDatabaseEntry(pairKey, pairValue));
        }
        databaseHandler.addTable(stream1);
        databaseHandler.updateAll(keyValuePairs, stream1);
        List<RemoteCorfuTableVersionedKey> keysToRead =
                keyValuePairs.stream().map(RemoteCorfuTableDatabaseEntry::getKey).collect(Collectors.toList());
        List<RemoteCorfuTableDatabaseEntry> readEntries = databaseHandler.multiGet(keysToRead, stream1);
        assertEquals(keyValuePairs, readEntries);
    }

    @Test
    public void testStreamIDNotInDatabase() throws RocksDBException {
        RemoteCorfuTableVersionedKey dummyKey = new RemoteCorfuTableVersionedKey(key1, 0L);
        ByteString dummyVal = ByteString.copyFrom("dummy".getBytes(DATABASE_CHARSET));
        databaseHandler.update(dummyKey,dummyVal,stream1);
        assertEquals(dummyVal, databaseHandler.get(dummyKey, stream1));
    }

    @Test
    public void testDatabaseComparator() throws RocksDBException, DatabaseOperationException {
        RemoteCorfuTableVersionedKey[] keys = new RemoteCorfuTableVersionedKey[6];
        byte[] prefix1 = new byte[]{0x12, 0x34, 0x56, 0x78};
        byte[] prefix2 = new byte[]{0x12, 0x34, 0x56, 0x78, 0x00};

        keys[0] = new RemoteCorfuTableVersionedKey(ByteString.copyFrom(prefix2), 0x03 << 8);
        keys[1] = new RemoteCorfuTableVersionedKey(ByteString.copyFrom(prefix2), 0x02 << 8);
        keys[2] = new RemoteCorfuTableVersionedKey(ByteString.copyFrom(prefix2), 0x01 << 8);
        keys[3] = new RemoteCorfuTableVersionedKey(ByteString.copyFrom(prefix1), 0x20 << 24);
        keys[4] = new RemoteCorfuTableVersionedKey(ByteString.copyFrom(prefix1), 0x01 << 24);
        keys[5] = new RemoteCorfuTableVersionedKey(ByteString.copyFrom(prefix1), 0L);
        List<RemoteCorfuTableDatabaseEntry> entriesToAdd = new LinkedList<>();
        for (int i = 0; i < 6; i++) {
            entriesToAdd.add(
                    new RemoteCorfuTableDatabaseEntry(keys[i], ByteString.copyFrom(("val" + i).getBytes(DATABASE_CHARSET))));
        }
        try {
            databaseHandler.addTable(stream1);
            databaseHandler.updateAll(entriesToAdd, stream1);
            List<RemoteCorfuTableDatabaseEntry> allEntries = databaseHandler.fullDatabaseScan(stream1);
            assertEquals( 6, allEntries.size());
            for (int i = 0; i < allEntries.size(); i++) {
                assertEquals(entriesToAdd.get(i), allEntries.get(i));
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test database ordering: ", e);
            throw e;
        }
    }

    @Test
    public void testDeleteRangeAll() throws RocksDBException, DatabaseOperationException {
        List<RemoteCorfuTableDatabaseEntry> entriesToAdd = new LinkedList<>();
        for (int i = 0; i < 6; i++) {
            entriesToAdd.add(
                    new RemoteCorfuTableDatabaseEntry(
                            new RemoteCorfuTableVersionedKey(key1,i),
                            ByteString.copyFrom(("ver" + i + "val").getBytes(DATABASE_CHARSET))));
        }
        try {
            databaseHandler.addTable(stream1);
            databaseHandler.updateAll(entriesToAdd, stream1);
            //Expected to result in deleting all versions of key1
            databaseHandler.delete(entriesToAdd.get(5).getKey(), entriesToAdd.get(0).getKey(),
                    true, stream1);
            //should be empty
            List<RemoteCorfuTableDatabaseEntry> allEntries = databaseHandler.fullDatabaseScan(stream1);
            assertEquals(0, allEntries.size());
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test full inclusive delete range: ", e);
            throw e;
        }
    }

    @Test
    public void testExclusiveEndDeleteRange() throws RocksDBException, DatabaseOperationException {
        List<RemoteCorfuTableDatabaseEntry> entriesToAdd = new LinkedList<>();
        for (int i = 0; i < 6; i++) {
            entriesToAdd.add(
                    new RemoteCorfuTableDatabaseEntry(
                            new RemoteCorfuTableVersionedKey(key1,i),
                            ByteString.copyFrom(("ver" + i + "val").getBytes(DATABASE_CHARSET))));
        }
        try {
            databaseHandler.addTable(stream1);
            databaseHandler.updateAll(entriesToAdd, stream1);
            //Expected to result in deleting all versions of key1 except ver0
            databaseHandler.delete(entriesToAdd.get(5).getKey(), entriesToAdd.get(0).getKey(),
                    false, stream1);
            //should have 1 element
            List<RemoteCorfuTableDatabaseEntry> allEntries = databaseHandler.fullDatabaseScan(stream1);
            assertEquals(1, allEntries.size());
            assertEquals(entriesToAdd.get(0), allEntries.get(0));
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in exclusive end delete range test: ", e);
            throw e;
        }
    }

    @Test
    public void testEndOfDataDeletion() throws RocksDBException {
        ByteString nullKeyPrefix = ByteString.copyFrom(new byte[]{0,0,0,0});
        List<RemoteCorfuTableDatabaseEntry> entriesToAdd = new LinkedList<>();
        for (int i = 0; i < 6; i++) {
            entriesToAdd.add(
                    new RemoteCorfuTableDatabaseEntry(
                            new RemoteCorfuTableVersionedKey(nullKeyPrefix,i),
                            ByteString.copyFrom(("ver" + i + "val").getBytes(DATABASE_CHARSET))));
        }
        try {
            databaseHandler.addTable(stream1);
            databaseHandler.updateAll(entriesToAdd, stream1);
            //Expected to result in deleting all versions of the null prefix
            databaseHandler.delete(entriesToAdd.get(5).getKey(), entriesToAdd.get(0).getKey(),
                    true, stream1);
            //should be empty
            List<RemoteCorfuTableDatabaseEntry> allEntries = databaseHandler.fullDatabaseScan(stream1);
            assertEquals(0, allEntries.size());
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test full inclusive delete range: ", e);
            throw e;
        }
    }

    @Test
    public void testDeleteFailsOnMultipleKeys() throws RocksDBException {
        RemoteCorfuTableVersionedKey firstKey =
                new RemoteCorfuTableVersionedKey(ByteString.copyFrom("First", DATABASE_CHARSET), 5L);
        RemoteCorfuTableVersionedKey secondKey =
                new RemoteCorfuTableVersionedKey(ByteString.copyFrom("Second", DATABASE_CHARSET), 0L);
        databaseHandler.addTable(stream1);
        assertThrows("Expected delete to throw DatabaseOperationException on invalid start/end keys",
                DatabaseOperationException.class,
                () -> databaseHandler.delete(firstKey, secondKey, true, stream1));
    }

    @Test
    public void testFullDBScanNoVersioning() throws RocksDBException, DatabaseOperationException {
        List<RemoteCorfuTableDatabaseEntry> entriesToAdd = new LinkedList<>();
        for (int i = 0; i < 1000; i++) {
            entriesToAdd.add(new RemoteCorfuTableDatabaseEntry(
                    new RemoteCorfuTableVersionedKey(ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                            Longs.toByteArray(i))), 0L),
                    ByteString.copyFrom(("val" + i).getBytes(DATABASE_CHARSET))));
        }
        try {
            databaseHandler.addTable(stream1);
            databaseHandler.updateAll(entriesToAdd, stream1);
            List<RemoteCorfuTableDatabaseEntry> fullDB = databaseHandler.scan(1000,stream1, 0);
            assertEquals(1000, fullDB.size());
            for (int i = 0; i < 1000; i++) {
                assertEquals(entriesToAdd.get(999-i), fullDB.get(i));
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in no version full DB scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testFullDBCursorScanNoVersioning() throws RocksDBException, DatabaseOperationException {
        List<RemoteCorfuTableDatabaseEntry> entriesToAdd = new LinkedList<>();
        for (int i = 0; i < 1000; i++) {
            entriesToAdd.add(new RemoteCorfuTableDatabaseEntry(
                    new RemoteCorfuTableVersionedKey(ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                            Longs.toByteArray(i))), 0L),
                    ByteString.copyFrom(("val" + i).getBytes(DATABASE_CHARSET))));
        }
        try {
            databaseHandler.addTable(stream1);
            databaseHandler.updateAll(entriesToAdd, stream1);
            List<RemoteCorfuTableDatabaseEntry> fullDB = new LinkedList<>();
            List<RemoteCorfuTableDatabaseEntry> currScan = null;
            boolean first = true;
            do {
                if (first) {
                    currScan = databaseHandler.scan(20, stream1, 0);
                    first = false;
                } else {
                    currScan = databaseHandler.scan(
                            currScan.get(currScan.size()-1).getKey(), 20, stream1,0);
                }
                fullDB.addAll(currScan);
            } while (currScan.size() >= 20);
            assertEquals(1000, fullDB.size());
            for (int i = 0; i < 1000; i++) {
                assertEquals(entriesToAdd.get(999-i), fullDB.get(i));
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in no version full DB cursor scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testFullDBScanWithVersion() throws RocksDBException, DatabaseOperationException {
        RemoteCorfuTableDatabaseEntry[][] entries = new RemoteCorfuTableDatabaseEntry[200][5];
        for (int i = 0; i < 200; i++) {
            for (int j = 0; j < 5; j++) {
                entries[i][j] = new RemoteCorfuTableDatabaseEntry(
                        new RemoteCorfuTableVersionedKey(ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                                Longs.toByteArray(i))), j),
                        ByteString.copyFrom(("val" + i + "ver" + j).getBytes(DATABASE_CHARSET))
                );
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 200; i++) {
                for (int j = 0; j < 5; j++) {
                    databaseHandler.update(entries[i][j].getKey(), entries[i][j].getValue(), stream1);
                }
            }
            for (int j = 0; j < 5; j++) {
                List<RemoteCorfuTableDatabaseEntry> allEntriesForVersion = databaseHandler.scan(200,stream1,j);
                assertEquals(200, allEntriesForVersion.size());
                for (int i = 0; i < 200; i++) {
                    assertEquals(entries[199-i][j], allEntriesForVersion.get(i));
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in version full DB scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testFullDBCursorScanWithVersion() throws RocksDBException, DatabaseOperationException {
        RemoteCorfuTableDatabaseEntry[][] entries = new RemoteCorfuTableDatabaseEntry[200][5];
        for (int i = 0; i < 200; i++) {
            for (int j = 0; j < 5; j++) {
                entries[i][j] = new RemoteCorfuTableDatabaseEntry(
                        new RemoteCorfuTableVersionedKey(ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                                Longs.toByteArray(i))), j),
                        ByteString.copyFrom(("val" + i + "ver" + j).getBytes(DATABASE_CHARSET))
                );
            }
        }
        List<RemoteCorfuTableDatabaseEntry> allEntriesForVersion;
        List<RemoteCorfuTableDatabaseEntry> currEntries = null;
        boolean first;
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 200; i++) {
                for (int j = 0; j < 5; j++) {
                    databaseHandler.update(entries[i][j].getKey(), entries[i][j].getValue(), stream1);
                }
            }
            for (int j = 0; j < 5; j++) {
                allEntriesForVersion = new LinkedList<>();
                first = true;
                do {
                    if (first) {
                        first = false;
                        currEntries = databaseHandler.scan(10,stream1,j);
                    } else {
                        currEntries = databaseHandler.scan(currEntries.get(currEntries.size()-1).getKey(),
                                10, stream1, j);
                    }
                    allEntriesForVersion.addAll(currEntries);
                } while (currEntries.size() >= 10);
                assertEquals(200, allEntriesForVersion.size());
                for (int i = 0; i < 200; i++) {
                    assertEquals(entries[199-i][j], allEntriesForVersion.get(i));
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in version full DB cursor scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testScanWithNullValuesNoVersioning() throws RocksDBException, DatabaseOperationException {
        List<RemoteCorfuTableDatabaseEntry> entries = new ArrayList<>(1000);
        int k = 1;
        int skip = k;
        for (int i = 0; i < 1000; i++) {
            RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(
                    ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                            Longs.toByteArray(i))), 0L
            );
            ByteString val;
            if (skip == 0) {
                k++;
                skip = k;
                val = ByteString.EMPTY;
            } else {
                val = ByteString.copyFrom(("val" + i).getBytes(DATABASE_CHARSET));
            }
            entries.add(new RemoteCorfuTableDatabaseEntry(key, val));
            skip--;
        }
        try {
            databaseHandler.addTable(stream1);
            databaseHandler.updateAll(entries, stream1);
            List<RemoteCorfuTableDatabaseEntry> nonNullValueEntries = new LinkedList<>();
            for (int i = 0; i < 1000; i++) {
                if (!entries.get(i).getValue().isEmpty()) {
                    nonNullValueEntries.add(entries.get(i));
                }
            }
            List<RemoteCorfuTableDatabaseEntry> allScannedEntries = databaseHandler.scan(1000, stream1, 0L);
            assertEquals(nonNullValueEntries.size(), allScannedEntries.size());
            for (int i = 0; i < allScannedEntries.size(); i++) {
                assertEquals(nonNullValueEntries.get(allScannedEntries.size()-1-i), allScannedEntries.get(i));
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in no version null values scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testScanWithNullValuesVersioning() throws RocksDBException, DatabaseOperationException {
        List<List<RemoteCorfuTableDatabaseEntry>> entries = new ArrayList<>(250);
        for (int i = 0; i < 250; i++) {
            entries.add(new ArrayList<>(4));
        }
        int k = 5;
        int skip = k;
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 250; i++) {
                RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(
                        ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                        Longs.toByteArray(i))), j);
                ByteString val;
                if (skip == 0) {
                    if (k == 0) {
                        k = 5;
                    } else {
                        k--;
                    }
                    skip = k;
                    val = ByteString.EMPTY;
                } else {
                    val = ByteString.copyFrom(("val" + i + "ver" + j).getBytes(DATABASE_CHARSET));
                }
                entries.get(i).add(new RemoteCorfuTableDatabaseEntry(key, val));
                skip--;
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 250; i++) {
                databaseHandler.updateAll(entries.get(i), stream1);
            }
            List<RemoteCorfuTableDatabaseEntry> nonNullValueEntriesByVersion;
            List<RemoteCorfuTableDatabaseEntry> scannedEntriesByVersion;
            for (int j = 0; j < 4; j++) {
                nonNullValueEntriesByVersion = new LinkedList<>();
                for (int i = 0; i < 250; i++) {
                    if (!entries.get(i).get(j).getValue().isEmpty()) {
                        nonNullValueEntriesByVersion.add(entries.get(i).get(j));
                    }
                }
                scannedEntriesByVersion = databaseHandler.scan(250, stream1, j);
                assertEquals(nonNullValueEntriesByVersion.size(), scannedEntriesByVersion.size());
                for (int i = 0; i < scannedEntriesByVersion.size(); i++) {
                    assertEquals(nonNullValueEntriesByVersion.get(scannedEntriesByVersion.size()-1-i),
                            scannedEntriesByVersion.get(i));
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in no version null values scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testClearFunctionality() throws RocksDBException {
        RemoteCorfuTableDatabaseEntry[][] entries = new RemoteCorfuTableDatabaseEntry[200][5];
        for (int i = 0; i < 200; i++) {
            for (int j = 0; j < 5; j++) {
                entries[i][j] = new RemoteCorfuTableDatabaseEntry(
                        new RemoteCorfuTableVersionedKey(
                                ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                                Longs.toByteArray(i))), j),
                        ByteString.copyFrom(("val" + i + "ver" + j).getBytes(DATABASE_CHARSET))
                );
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 200; i++) {
                for (int j = 0; j < 5; j++) {
                    databaseHandler.update(entries[i][j].getKey(), entries[i][j].getValue(), stream1);
                }
            }
            databaseHandler.clear(stream1,5);
            for (int j = 0; j < 5; j++) {
                List<RemoteCorfuTableDatabaseEntry> allEntriesForVersion = databaseHandler.scan(200,stream1,j);
                assertEquals(200, allEntriesForVersion.size());
                for (int i = 0; i < 200; i++) {
                    assertEquals(entries[199-i][j], allEntriesForVersion.get(i));
                }
            }
            List<RemoteCorfuTableDatabaseEntry> version5Entries = databaseHandler.scan(200, stream1, 5);
            assertEquals(0, version5Entries.size());
            List<RemoteCorfuTableDatabaseEntry> allEntries = databaseHandler.fullDatabaseScan(stream1);
            assertEquals(1200, allEntries.size());
            for (int i = 0; i < 200; i++) {
                assertEquals(
                        new RemoteCorfuTableDatabaseEntry(
                                new RemoteCorfuTableVersionedKey(entries[199-i][0].getKey().getEncodedKey(), 5L),
                                ByteString.EMPTY
                        ),
                        allEntries.get(i*6));
                for (int j = 0; j < 5; j++) {
                    assertEquals(entries[199-i][j], allEntries.get(i*6 + (5-j)));
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in clear test: ", e);
            throw e;
        }
    }

    @Test
    public void testContainsKeyFunctionality() throws RocksDBException {
        List<List<RemoteCorfuTableDatabaseEntry>> entries = new ArrayList<>(250);
        for (int i = 0; i < 250; i++) {
            entries.add(new ArrayList<>(4));
        }
        int k = 5;
        int skip = k;
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 250; i++) {
                RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(
                        ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET), Longs.toByteArray(i))), j);
                ByteString val;
                if (skip == 0) {
                    if (k == 0) {
                        k = 5;
                    } else {
                        k--;
                    }
                    skip = k;
                    val = ByteString.EMPTY;
                } else {
                    val = ByteString.copyFrom(("val" + i + "ver" + j).getBytes(DATABASE_CHARSET));
                }
                entries.get(i).add(new RemoteCorfuTableDatabaseEntry(key, val));
                skip--;
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 250; i++) {
                databaseHandler.updateAll(entries.get(i), stream1);
            }
            for (int i = 0; i < 250; i++) {
                for (int j = 0; j < 4; j++) {
                    if (entries.get(i).get(j).getValue().isEmpty()) {
                        assertFalse(databaseHandler.containsKey(entries.get(i).get(j).getKey(), stream1));
                    } else {
                        assertTrue(databaseHandler.containsKey(entries.get(i).get(j).getKey(), stream1));
                    }
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in contains key test: ", e);
            throw e;
        }
    }

    @Test
    public void testContainsValueFunctionality() throws RocksDBException {
        List<List<RemoteCorfuTableDatabaseEntry>> entries = new ArrayList<>(10);
        for (int i = 0; i < 2; i++) {
            entries.add(new ArrayList<>(2));
        }
        List<ByteString> vals = IntStream.range(0,1000)
                .mapToObj(j -> ByteString.copyFrom("val" + j, DATABASE_CHARSET)).collect(Collectors.toList());
        RemoteCorfuTableVersionedKey key;
        ByteString value;
        for (int i = 0; i < 1000; i++) {
            if (i%2 == 1) {
                key = new RemoteCorfuTableVersionedKey(
                        ByteString.copyFrom("key" + i, DATABASE_CHARSET), 1L);
                value = vals.get(i);
                entries.get(1).add(new RemoteCorfuTableDatabaseEntry(key, value));
            } else {
                key = new RemoteCorfuTableVersionedKey(
                        ByteString.copyFrom("key" + i, DATABASE_CHARSET), 0L);
                value = vals.get(i);
                entries.get(0).add(new RemoteCorfuTableDatabaseEntry(key, value));
            }
        }
        databaseHandler.addTable(stream1);
        databaseHandler.updateAll(entries.get(1), stream1);
        databaseHandler.updateAll(entries.get(0), stream1);

        List<List<Boolean>> resultsByVersion = new ArrayList<>(2);
        for (int i = 0; i < 2; i++) {
            resultsByVersion.add(new ArrayList<>(10));
        }
        for (ByteString val : vals) {
            resultsByVersion.get(0).add(databaseHandler.containsValue(val, stream1, 0L, 3));
            resultsByVersion.get(1).add(databaseHandler.containsValue(val, stream1, 1L, 3));
        }
        int failed = 0;
        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 2; j++) {
                boolean val = resultsByVersion.get(j).get(i);
                if ((!val && j == 1) || (!val && j == 0 && (i%2 == 0))) {
                    failed++;
                    System.out.println("Failed value at index " + i + ", " + j  + ": " + val);
                }
            }
        }
        assertEquals(0,failed);
    }

    //Intermittently throws UnsupportedOperationException for ByteBuffer.array
    // may be related to https://github.com/facebook/rocksdb/issues/6608, with C++ handling issues

    @Test
    public void testContainsValueFunctionalityAdvanced() throws RocksDBException {
        List<List<RemoteCorfuTableDatabaseEntry>> entries = new ArrayList<>(250);
        List<List<ByteString>> values = new ArrayList<>(250);
        for (int i = 0; i < 250; i++) {
            entries.add(new ArrayList<>(4));
            values.add(new ArrayList<>(4));
        }
        int k = 5;
        int skip = k;
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 250; i++) {
                RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(
                        ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET), Longs.toByteArray(i))), j);
                ByteString val = ByteString.copyFrom(("val" + i + "ver" + j).getBytes(DATABASE_CHARSET));
                values.get(i).add(val);
                if (skip == 0) {
                    if (k == 0) {
                        k = 5;
                    } else {
                        k--;
                    }
                    skip = k;
                    val = ByteString.EMPTY;
                }
                entries.get(i).add(new RemoteCorfuTableDatabaseEntry(key, val));
                skip--;
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 250; i++) {
                databaseHandler.updateAll(entries.get(i), stream1);
            }
            List<String> failures = new LinkedList<>();
            for (int i = 0; i < 250; i++) {
                List<ByteString> addedVals = entries.get(i).stream()
                        .map(RemoteCorfuTableDatabaseEntry::getValue).collect(Collectors.toList());
                List<ByteString> versionVals = values.get(i);
                for (int j = 0; j < 4; j++) {
                    if (addedVals.get(j).isEmpty()) {
                        if (databaseHandler.containsValue(versionVals.get(j), stream1, j, 17)) {
                            failures.add(String.format("Failed with TRUE value at i: %d, j: %d\n", i, j));
                        }
                    } else {
                        if (!databaseHandler.containsValue(versionVals.get(j), stream1, j, 17)) {
                            failures.add(String.format("Failed with FALSE value at i: %d, j: %d\n", i, j));
                        }
                    }
                }
            }
            failures.forEach(System.out::println);
            assertEquals(0, failures.size());
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in contains value test: ", e);
            throw e;
        }
    }

    @Test
    public void testSizeFunctionality() throws RocksDBException {
        List<List<RemoteCorfuTableDatabaseEntry>> entries = new ArrayList<>(250);
        for (int i = 0; i < 250; i++) {
            entries.add(new ArrayList<>(4));
        }
        int k = 5;
        int skip = k;
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 250; i++) {
                RemoteCorfuTableVersionedKey key = new RemoteCorfuTableVersionedKey(
                        ByteString.copyFrom(Bytes.concat("key".getBytes(DATABASE_CHARSET), Longs.toByteArray(i))), j);
                ByteString val;
                if (skip == 0) {
                    if (k == 0) {
                        k = 5;
                    } else {
                        k--;
                    }
                    skip = k;
                    val = ByteString.EMPTY;
                } else {
                    val = ByteString.copyFrom(("val" + i + "ver" + j).getBytes(DATABASE_CHARSET));
                }
                entries.get(i).add(new RemoteCorfuTableDatabaseEntry(key, val));
                skip--;
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 250; i++) {
                databaseHandler.updateAll(entries.get(i), stream1);
            }
            List<RemoteCorfuTableDatabaseEntry> nonNullValueEntriesByVersion;
            for (int j = 0; j < 4; j++) {
                nonNullValueEntriesByVersion = new LinkedList<>();
                for (int i = 0; i < 250; i++) {
                    if (!entries.get(i).get(j).getValue().isEmpty()) {
                        nonNullValueEntriesByVersion.add(entries.get(i).get(j));
                    }
                }
                assertEquals(nonNullValueEntriesByVersion.size(), databaseHandler.size(stream1,j,10));
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in size test: ", e);
            throw e;
        }
    }
}
