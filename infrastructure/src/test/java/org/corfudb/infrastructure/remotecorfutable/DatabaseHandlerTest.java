package org.corfudb.infrastructure.remotecorfutable;

import com.google.protobuf.ByteString;
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
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.DATABASE_CHARSET;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
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
    private final ByteString stream1 = ByteString.copyFrom("stream1", DATABASE_CHARSET);
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
                        assertArrayEquals(readinVal,v4Val);
                        break;
                    case 3: case 2:
                        assertArrayEquals(readinVal,v2Val);
                        break;
                    case 1:
                        assertArrayEquals(readinVal,v1Val);
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
    public void testStreamIDNotInDatabase() throws RocksDBException, DatabaseOperationException {
        byte[] dummyVal = "dummy".getBytes(DATABASE_CHARSET);
        assertThrows("Expected PUT to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.update(key1,dummyVal,stream1));
        assertThrows("Expected GET to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.get(key1,stream1));
        assertThrows("Expected DELETE to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.delete(key1,dummyVal,stream1));
        assertThrows("Expected PUT to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.update(key1,dummyVal,stream1));
        assertThrows("Expected SCAN to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.scan(stream1,0L));
        assertThrows("Expected CONTAINSKEY to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.containsKey(key1,stream1));
        assertThrows("Expected CONTAINSVALUE to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.containsValue(dummyVal,stream1,0L,10));
        assertThrows("Expected SIZE to throw StreamID not found error",
                DatabaseOperationException.class,() -> databaseHandler.size(stream1,0L,10));
    }

    @Test
    public void testDatabaseOrdering() throws RocksDBException, DatabaseOperationException {
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
            List<byte[][]> allEntries = databaseHandler.fullDatabaseScan(stream1);
            for (byte[][] entry : allEntries) {
                KeyEncodingUtil.VersionedKey key = KeyEncodingUtil.extractVersionedKey(entry[0]);
                String val = new String(entry[1], DATABASE_CHARSET);
                System.out.println(String.format("%s, %s",key,val));
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test database ordering: ", e);
            throw e;
        }
    }
}
