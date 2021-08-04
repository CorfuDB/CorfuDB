package org.corfudb.infrastructure.remotecorfutable;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.remotecorfutable.utils.KeyEncodingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;

import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.DATABASE_CHARSET;
import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.EMPTY_VALUE;
import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.isEmpty;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    private ThreadPoolExecutor mThreadPoolExecutor;

    //constants
    private final UUID stream1 = UUID.nameUUIDFromBytes("stream1".getBytes(DATABASE_CHARSET));
    private final byte[] key1 = "key1".getBytes(DATABASE_CHARSET);

    /**
     * Mocks required objects and sets up database handler.
     */
    @Before
    public void setup() {
        mPath = mock(Path.class);
        mOptions = new Options();
        mOptions.setCreateIfMissing(true);
        mOptions.setComparator(BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR);
        mThreadPoolExecutor = mock(ThreadPoolExecutor.class);
        mFile = mock(File.class);

        String TEST_TEMP_DIR = com.google.common.io.Files.createTempDir().getAbsolutePath();

        when(mFile.getAbsolutePath()).thenReturn(TEST_TEMP_DIR);
        when(mPath.toFile()).thenReturn(mFile);

        databaseHandler = new DatabaseHandler(mPath,mOptions,mThreadPoolExecutor);
    }

    @After
    public void dispose() {
        databaseHandler.close();
    }

    @Test
    public void testPutandGetBasicFunctionality() throws RocksDBException, DatabaseOperationException {
        byte[] expectedValue = "ver0val".getBytes(DATABASE_CHARSET);
        byte[] encodedKey = KeyEncodingUtil.constructDatabaseKey(key1,0L);
        byte[] readinVal;
        try {
            databaseHandler.addTable(stream1);
            databaseHandler.update(encodedKey,expectedValue,stream1);
            readinVal = databaseHandler.get(encodedKey,stream1);
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test PUT and GET Basic: ", e);
            throw e;
        }
        assertArrayEquals(expectedValue,readinVal);
    }

    @Test
    public void testVersionedGetFunctionality() throws RocksDBException, DatabaseOperationException {
        byte[] v1Val = "ver1val".getBytes(DATABASE_CHARSET);
        byte[] v2Val = "ver2val".getBytes(DATABASE_CHARSET);
        byte[] v4Val = "ver1val".getBytes(DATABASE_CHARSET);
        byte[][] keys = new byte[6][];
        for (int i = 0; i < 6; i++) {
            keys[i] = KeyEncodingUtil.constructDatabaseKey(key1,i);
        }
        byte[] readinVal;
        try {
            databaseHandler.addTable(stream1);
            databaseHandler.update(keys[1],v1Val,stream1);
            databaseHandler.update(keys[2],v2Val,stream1);
            databaseHandler.update(keys[4],v4Val,stream1);
            for (int i = 0; i < 6; i++) {
                readinVal = databaseHandler.get(keys[i],stream1);
                switch (i) {
                    case 5: case 4:
                        assertArrayEquals(v4Val, readinVal);
                        break;
                    case 3: case 2:
                        assertArrayEquals(v2Val, readinVal);
                        break;
                    case 1:
                        assertArrayEquals(v1Val, readinVal);
                        break;
                    default:
                        assertNull(readinVal);
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test versioned GET: ", e);
            throw e;
        }
    }

    @Test
    public void testUpdateAllBasic() throws RocksDBException {
        List<byte[][]> keyValuePairs = new LinkedList<>();
        for (int i = 0; i < 1000; i++) {
            byte[][] pair = new byte[2][];
            pair[0] = KeyEncodingUtil.constructDatabaseKey(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                    Longs.toByteArray(i)), 0L);
            pair[1] = ("val" + i).getBytes(DATABASE_CHARSET);
            keyValuePairs.add(pair);
        }
        databaseHandler.addTable(stream1);
        databaseHandler.updateAll(keyValuePairs, stream1);
        List<byte[][]> read = databaseHandler.fullDatabaseScan(stream1);
        for (int i = 0; i < 1000; i++) {
            assertArrayEquals(keyValuePairs.get(i)[0],read.get(999-i)[0]);
            assertArrayEquals(keyValuePairs.get(i)[1],read.get(999-i)[1]);
        }
    }

    @Test
    public void testStreamIDNotInDatabase() {
        byte[] dummyVal = "dummy".getBytes(DATABASE_CHARSET);
        assertThrows("Expected PUT to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.update(key1,dummyVal,stream1));
        assertThrows("Expected GET to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.get(key1,stream1));
        assertThrows("Expected DELETE to throw StreamID not found error",
                DatabaseOperationException.class,
                () -> databaseHandler.delete(key1,dummyVal, true, stream1));
        assertThrows("Expected PUT to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.update(key1,dummyVal,stream1));
        assertThrows("Expected SCAN to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.scan(stream1,0L));
        assertThrows("Expected CONTAINSKEY to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.containsKey(key1,stream1));
        assertThrows("Expected CONTAINSVALUE to throw StreamID not found error",
                DatabaseOperationException.class,
                () -> databaseHandler.containsValue(dummyVal,stream1,0L,10));
        assertThrows("Expected SIZE to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.size(stream1,0L,10));
    }

    @Test
    public void testDatabaseComparator() throws RocksDBException, DatabaseOperationException {
        byte[][] keys = new byte[6][];
        byte[][] vals = new byte[6][];
        byte[] prefix1 = new byte[]{0x12, 0x34, 0x56, 0x78};
        byte[] prefix2 = new byte[]{0x12, 0x34, 0x56, 0x78, 0x00};

        keys[0] = KeyEncodingUtil.constructDatabaseKey(prefix2, 0x03 << 8);
        keys[1] = KeyEncodingUtil.constructDatabaseKey(prefix2, 0x02 << 8);
        keys[2] = KeyEncodingUtil.constructDatabaseKey(prefix2, 0x01 << 8);
        keys[3] = KeyEncodingUtil.constructDatabaseKey(prefix1, 0x20 << 24);
        keys[4] = KeyEncodingUtil.constructDatabaseKey(prefix1, 0x01 << 24);
        keys[5] = KeyEncodingUtil.constructDatabaseKey(prefix1, 0L);
        for (int i = 0; i < 6; i++) {
            vals[i] = ("val" + i).getBytes(DATABASE_CHARSET);
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 6; i++) {
                databaseHandler.update(keys[i], vals[i], stream1);
            }
            List<byte[][]> allEntries = databaseHandler.fullDatabaseScan(stream1);
            assertEquals( 6, allEntries.size());
            for (int i = 0; i < allEntries.size(); i++) {
                assertArrayEquals(keys[i], allEntries.get(i)[0]);
                assertArrayEquals( vals[i], allEntries.get(i)[1]);
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test database ordering: ", e);
            throw e;
        }
    }

    @Test
    public void testDeleteRangeAll() throws RocksDBException, DatabaseOperationException {
        byte[][] keys = new byte[6][];
        byte[][] vals = new byte[6][];
        for (int i = 0; i < 6; i++) {
            keys[i] = KeyEncodingUtil.constructDatabaseKey(key1,i);
            vals[i] = ("ver" + i + "val").getBytes(DATABASE_CHARSET);
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 6; i++) {
                databaseHandler.update(keys[i], vals[i], stream1);
            }
            //Expected to result in deleting all versions of key1
            databaseHandler.delete(keys[5], keys[0], true, stream1);
            //should be empty
            List<byte[][]> allEntries = databaseHandler.fullDatabaseScan(stream1);
            assertEquals(0, allEntries.size());
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test full inclusive delete range: ", e);
            throw e;
        }
    }

    @Test
    public void testExclusiveEndDeleteRange() throws RocksDBException, DatabaseOperationException {
        byte[][] keys = new byte[6][];
        byte[][] vals = new byte[6][];
        for (int i = 0; i < 6; i++) {
            keys[i] = KeyEncodingUtil.constructDatabaseKey(key1,i);
            vals[i] = ("ver" + i + "val").getBytes(DATABASE_CHARSET);
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 6; i++) {
                databaseHandler.update(keys[i], vals[i], stream1);
            }
            //Expected to result in deleting all versions of key1 except ver0
            databaseHandler.delete(keys[5], keys[0], false,stream1);
            //should have 1 element
            List<byte[][]> allEntries = databaseHandler.fullDatabaseScan(stream1);
            assertEquals(1, allEntries.size());
            assertArrayEquals(keys[0], allEntries.get(0)[0]);
            assertArrayEquals(vals[0], allEntries.get(0)[1]);
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in exclusive end delete range test: ", e);
            throw e;
        }
    }

    @Test
    public void testEndOfDataDeletion() throws RocksDBException {
        byte[][] keys = new byte[6][];
        byte[][] vals = new byte[6][];
        byte[] nullKeyPrefix = new byte[]{0,0,0,0};
        for (int i = 0; i < 6; i++) {
            keys[i] = KeyEncodingUtil.constructDatabaseKey(nullKeyPrefix,i);
            vals[i] = ("ver" + i + "val").getBytes(DATABASE_CHARSET);
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 6; i++) {
                databaseHandler.update(keys[i], vals[i], stream1);
            }
            //Expected to result in deleting all versions of the null prefix
            databaseHandler.delete(keys[5], keys[0], true, stream1);
            //should be empty
            List<byte[][]> allEntries = databaseHandler.fullDatabaseScan(stream1);
            assertEquals(0, allEntries.size());
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test full inclusive delete range: ", e);
            throw e;
        }
    }

    @Test
    public void testFullDBScanNoVersioning() throws RocksDBException, DatabaseOperationException {
        byte[][] keys = new byte[1000][];
        byte[][] vals = new byte[1000][];
        for (int i = 0; i < 1000; i++) {
            keys[i] = KeyEncodingUtil.constructDatabaseKey(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                    Longs.toByteArray(i)), 0L);
            vals[i] = ("val" + i).getBytes(DATABASE_CHARSET);
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 1000; i++) {
                databaseHandler.update(keys[i],vals[i],stream1);
            }
            List<byte[][]> fullDB = databaseHandler.scan(1000,stream1, 0);
            assertEquals(1000, fullDB.size());
            for (int i = 0; i < 1000; i++) {
                assertArrayEquals(keys[999-i], fullDB.get(i)[0]);
                assertArrayEquals(vals[999-i], fullDB.get(i)[1]);
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in no version full DB scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testFullDBCursorScanNoVersioning() throws RocksDBException, DatabaseOperationException {
        byte[][] keys = new byte[1000][];
        byte[][] vals = new byte[1000][];
        for (int i = 0; i < 1000; i++) {
            keys[i] = KeyEncodingUtil.constructDatabaseKey(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                    Longs.toByteArray(i)), 0L);
            vals[i] = ("val" + i).getBytes(DATABASE_CHARSET);
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 1000; i++) {
                databaseHandler.update(keys[i],vals[i],stream1);
            }
            List<byte[][]> fullDB = new LinkedList<>();
            List<byte[][]> currScan = null;
            boolean first = true;
            do {
                if (first) {
                    currScan = databaseHandler.scan(20, stream1, 0);
                    first = false;
                } else {
                    currScan = databaseHandler.scan(currScan.get(currScan.size()-1)[0], 20, stream1);
                }
                fullDB.addAll(currScan);
            } while (currScan.size() >= 20);
            assertEquals(1000, fullDB.size());
            for (int i = 0; i < 1000; i++) {
                assertArrayEquals(keys[999-i], fullDB.get(i)[0]);
                assertArrayEquals(vals[999-i], fullDB.get(i)[1]);
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in no version full DB cursor scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testFullDBScanWithVersion() throws RocksDBException, DatabaseOperationException {
        byte[][][] keys = new byte[200][5][];
        byte[][][] vals = new byte[200][5][];
        for (int i = 0; i < 200; i++) {
            for (int j = 0; j < 5; j++) {
                keys[i][j] = KeyEncodingUtil.constructDatabaseKey(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                        Longs.toByteArray(i)), j);
                vals[i][j] = ("val" + i + "ver" + j).getBytes(DATABASE_CHARSET);
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 200; i++) {
                for (int j = 0; j < 5; j++) {
                    databaseHandler.update(keys[i][j], vals[i][j], stream1);
                }
            }
            for (int j = 0; j < 5; j++) {
                List<byte[][]> allEntriesForVersion = databaseHandler.scan(200,stream1,j);
                assertEquals(200, allEntriesForVersion.size());
                for (int i = 0; i < 200; i++) {
                    assertArrayEquals(keys[199-i][j], allEntriesForVersion.get(i)[0]);
                    assertArrayEquals(vals[199-i][j], allEntriesForVersion.get(i)[1]);
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in version full DB scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testFullDBCursorScanWithVersion() throws RocksDBException, DatabaseOperationException {
        byte[][][] keys = new byte[200][5][];
        byte[][][] vals = new byte[200][5][];
        for (int i = 0; i < 200; i++) {
            for (int j = 0; j < 5; j++) {
                keys[i][j] = KeyEncodingUtil.constructDatabaseKey(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                        Longs.toByteArray(i)), j);
                vals[i][j] = ("val" + i + "ver" + j).getBytes(DATABASE_CHARSET);
            }
        }
        List<byte[][]> allEntriesForVersion;
        List<byte[][]> currEntries = null;
        boolean first;
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 200; i++) {
                for (int j = 0; j < 5; j++) {
                    databaseHandler.update(keys[i][j], vals[i][j], stream1);
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
                        currEntries = databaseHandler.scan(currEntries.get(currEntries.size()-1)[0],
                                10, stream1);
                    }
                    allEntriesForVersion.addAll(currEntries);
                } while (currEntries.size() >= 10);
                assertEquals(200, allEntriesForVersion.size());
                for (int i = 0; i < 200; i++) {
                    assertArrayEquals(keys[199-i][j], allEntriesForVersion.get(i)[0]);
                    assertArrayEquals(vals[199-i][j], allEntriesForVersion.get(i)[1]);
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in version full DB cursor scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testScanWithNullValuesNoVersioning() throws RocksDBException, DatabaseOperationException {
        List<byte[]> keys = new ArrayList<>(1000);
        List<byte[]> vals = new ArrayList<>(1000);
        int k = 1;
        int skip = k;
        for (int i = 0; i < 1000; i++) {
            keys.add(KeyEncodingUtil.constructDatabaseKey(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                    Longs.toByteArray(i)), 0L));
            if (skip == 0) {
                k++;
                skip = k;
                vals.add(EMPTY_VALUE);
            } else {
                vals.add(("val" + i).getBytes(DATABASE_CHARSET));
            }
            skip--;
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 1000; i++) {
                databaseHandler.update(keys.get(i), vals.get(i), stream1);
            }
            List<byte[]> nonNullKeys = new LinkedList<>();
            List<byte[]> nonNullValues = new LinkedList<>();
            for (int i = 0; i < 1000; i++) {
                if (!isEmpty(vals.get(i))) {
                    nonNullKeys.add(keys.get(i));
                    nonNullValues.add(vals.get(i));
                }
            }
            List<byte[][]> allScannedEntries = databaseHandler.scan(1000, stream1, 0L);
            assertEquals(nonNullKeys.size(), allScannedEntries.size());
            for (int i = 0; i < allScannedEntries.size(); i++) {
                assertArrayEquals(nonNullKeys.get(allScannedEntries.size()-1-i), allScannedEntries.get(i)[0]);
                assertArrayEquals(nonNullValues.get(allScannedEntries.size()-1-i), allScannedEntries.get(i)[1]);
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in no version null values scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testScanWithNullValuesVersioning() throws RocksDBException, DatabaseOperationException {
        List<List<byte[]>> keys = new ArrayList<>(250);
        List<List<byte[]>> vals = new ArrayList<>(250);
        for (int i = 0; i < 250; i++) {
            keys.add(new ArrayList<>(4));
            vals.add(new ArrayList<>(4));
        }
        int k = 5;
        int skip = k;
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 250; i++) {
                keys.get(i).add(KeyEncodingUtil.constructDatabaseKey(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                        Longs.toByteArray(i)), j));
                if (skip == 0) {
                    if (k == 0) {
                        k = 5;
                    } else {
                        k--;
                    }
                    skip = k;
                    vals.get(i).add(EMPTY_VALUE);
                } else {
                    vals.get(i).add(("val" + i + "ver" + j).getBytes(DATABASE_CHARSET));
                }
                skip--;
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 250; i++) {
                for (int j = 0; j < 4; j++) {
                    databaseHandler.update(keys.get(i).get(j), vals.get(i).get(j), stream1);
                }
            }
            List<byte[]> nonNullKeysByVersion;
            List<byte[]> nonNullValsByVersion;
            List<byte[][]> scannedEntriesByVersion;
            for (int j = 0; j < 4; j++) {
                nonNullKeysByVersion = new LinkedList<>();
                nonNullValsByVersion = new LinkedList<>();
                for (int i = 0; i < 250; i++) {
                    if (!isEmpty(vals.get(i).get(j))) {
                        nonNullKeysByVersion.add(keys.get(i).get(j));
                        nonNullValsByVersion.add(vals.get(i).get(j));
                    } else {
                        for (int l = j-1; l >= 0; l--) {
                            if (!isEmpty(vals.get(i).get(l))) {
                                nonNullKeysByVersion.add(keys.get(i).get(l));
                                nonNullValsByVersion.add(vals.get(i).get(l));
                                break;
                            }
                        }
                    }
                }
                scannedEntriesByVersion = databaseHandler.scan(250, stream1, j);
                assertEquals(nonNullKeysByVersion.size(), scannedEntriesByVersion.size());
                for (int i = 0; i < scannedEntriesByVersion.size(); i++) {
                    assertArrayEquals(nonNullKeysByVersion.get(scannedEntriesByVersion.size()-1-i),
                            scannedEntriesByVersion.get(i)[0]);
                    assertArrayEquals(nonNullValsByVersion.get(scannedEntriesByVersion.size()-1-i),
                            scannedEntriesByVersion.get(i)[1]);
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in no version null values scan test: ", e);
            throw e;
        }
    }

    @Test
    public void testClearFunctionality() throws RocksDBException {
        byte[][][] keys = new byte[200][5][];
        byte[][][] vals = new byte[200][5][];
        for (int i = 0; i < 200; i++) {
            for (int j = 0; j < 5; j++) {
                keys[i][j] = KeyEncodingUtil.constructDatabaseKey(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                        Longs.toByteArray(i)), j);
                vals[i][j] = ("val" + i + "ver" + j).getBytes(DATABASE_CHARSET);
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 200; i++) {
                for (int j = 0; j < 5; j++) {
                    databaseHandler.update(keys[i][j], vals[i][j], stream1);
                }
            }
            databaseHandler.clear(stream1,5);
            for (int j = 0; j < 5; j++) {
                List<byte[][]> allEntriesForVersion = databaseHandler.scan(200,stream1,j);
                assertEquals(200, allEntriesForVersion.size());
                for (int i = 0; i < 200; i++) {
                    assertArrayEquals(keys[199-i][j], allEntriesForVersion.get(i)[0]);
                    assertArrayEquals(vals[199-i][j], allEntriesForVersion.get(i)[1]);
                }
            }
            List<byte[][]> version5Entries = databaseHandler.scan(200, stream1, 5);
            assertEquals(0, version5Entries.size());
            List<byte[][]> allEntries = databaseHandler.fullDatabaseScan(stream1);
            assertEquals(1200, allEntries.size());
            for (int i = 0; i < 200; i++) {
                assertArrayEquals(
                    KeyEncodingUtil.constructDatabaseKey(
                            KeyEncodingUtil.extractEncodedKey(keys[199-i][0]), 5L),
                    allEntries.get(i*6)[0]);
                assertArrayEquals(EMPTY_VALUE, allEntries.get(i*6)[1]);
                for (int j = 0; j < 5; j++) {
                    assertArrayEquals(keys[199-i][j], allEntries.get(i*6 + (5-j))[0]);
                    assertArrayEquals(vals[199-i][j], allEntries.get(i*6 + (5-j))[1]);
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in clear test: ", e);
            throw e;
        }
    }

    @Test
    public void testContainsKeyFunctionality() throws RocksDBException {
        List<List<byte[]>> keys = new ArrayList<>(250);
        List<List<byte[]>> vals = new ArrayList<>(250);
        for (int i = 0; i < 250; i++) {
            keys.add(new ArrayList<>(4));
            vals.add(new ArrayList<>(4));
        }
        int k = 5;
        int skip = k;
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 250; i++) {
                keys.get(i).add(KeyEncodingUtil.constructDatabaseKey(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                        Longs.toByteArray(i)), j));
                if (skip == 0) {
                    if (k == 0) {
                        k = 5;
                    } else {
                        k--;
                    }
                    skip = k;
                    vals.get(i).add(EMPTY_VALUE);
                } else {
                    vals.get(i).add(("val" + i + "ver" + j).getBytes(DATABASE_CHARSET));
                }
                skip--;
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 250; i++) {
                for (int j = 0; j < 4; j++) {
                    databaseHandler.update(keys.get(i).get(j), vals.get(i).get(j), stream1);
                }
            }
            for (int i = 0; i < 250; i++) {
                for (int j = 0; j < 4; j++) {
                    if (isEmpty(vals.get(i).get(j))) {
                        assertFalse(databaseHandler.containsKey(keys.get(i).get(j), stream1));
                    } else {
                        assertTrue(databaseHandler.containsKey(keys.get(i).get(j), stream1));
                    }
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in contains key test: ", e);
            throw e;
        }
    }

    //Intermittently throws UnsupportedOperationException for ByteBuffer.array, does not seem to be affecting test performance
    // may be related to https://github.com/facebook/rocksdb/issues/6608, with C++ handling issues

    @Test
    public void testContainsValueFunctionality() throws RocksDBException {
        List<List<byte[]>> keys = new ArrayList<>(250);
        List<List<byte[]>> vals = new ArrayList<>(250);
        for (int i = 0; i < 250; i++) {
            keys.add(new ArrayList<>(4));
            vals.add(new ArrayList<>(4));
        }
        int k = 5;
        int skip = k;
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 250; i++) {
                keys.get(i).add(KeyEncodingUtil.constructDatabaseKey(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                        Longs.toByteArray(i)), j));
                if (skip == 0) {
                    if (k == 0) {
                        k = 5;
                    } else {
                        k--;
                    }
                    skip = k;
                    vals.get(i).add(EMPTY_VALUE);
                } else {
                    vals.get(i).add(("val" + i + "ver" + j).getBytes(DATABASE_CHARSET));
                }
                skip--;
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 250; i++) {
                for (int j = 0; j < 4; j++) {
                    databaseHandler.update(keys.get(i).get(j), vals.get(i).get(j), stream1);
                }
            }
            for (int i = 0; i < 250; i++) {
                List<byte[]> versionVals = vals.get(i);
                for (int j = 0; j < 4; j++) {
                    for (int l = 0; l < 4; l++) {
                        assert isEmpty(versionVals.get(l)) || (databaseHandler.containsValue(versionVals.get(l),
                                stream1, j, 10) == (l == j));
                    }
                }
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in contains value test: ", e);
            throw e;
        }
    }

    @Test
    public void testSizeFunctionality() throws RocksDBException {
        List<List<byte[]>> keys = new ArrayList<>(250);
        List<List<byte[]>> vals = new ArrayList<>(250);
        for (int i = 0; i < 250; i++) {
            keys.add(new ArrayList<>(4));
            vals.add(new ArrayList<>(4));
        }
        int k = 5;
        int skip = k;
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 250; i++) {
                keys.get(i).add(KeyEncodingUtil.constructDatabaseKey(Bytes.concat("key".getBytes(DATABASE_CHARSET),
                        Longs.toByteArray(i)), j));
                if (skip == 0) {
                    if (k == 0) {
                        k = 5;
                    } else {
                        k--;
                    }
                    skip = k;
                    vals.get(i).add(EMPTY_VALUE);
                } else {
                    vals.get(i).add(("val" + i + "ver" + j).getBytes(DATABASE_CHARSET));
                }
                skip--;
            }
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 250; i++) {
                for (int j = 0; j < 4; j++) {
                    databaseHandler.update(keys.get(i).get(j), vals.get(i).get(j), stream1);
                }
            }
            List<byte[]> nonNullKeysByVersion;
            for (int j = 0; j < 4; j++) {
                nonNullKeysByVersion = new LinkedList<>();
                for (int i = 0; i < 250; i++) {
                    if (!isEmpty(vals.get(i).get(j))) {
                        nonNullKeysByVersion.add(keys.get(i).get(j));
                    } else {
                        for (int l = j-1; l >= 0; l--) {
                            if (!isEmpty(vals.get(i).get(l))) {
                                nonNullKeysByVersion.add(keys.get(i).get(l));
                                break;
                            }
                        }
                    }
                }
                assertEquals(nonNullKeysByVersion.size(), databaseHandler.size(stream1,j,10));
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in size test: ", e);
            throw e;
        }
    }
}
