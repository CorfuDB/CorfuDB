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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.METADATA_CHARSET;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
    private final ByteString stream1 = ByteString.copyFrom("stream1", METADATA_CHARSET);
    private final byte[] key1 = "key1".getBytes(METADATA_CHARSET);

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
        byte[] expectedValue = "ver0val".getBytes(METADATA_CHARSET);
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

    //TODO: test is failing, check reverse comparator in db
    @Test
    public void testVersionedGetFunctionality() throws RocksDBException, DatabaseOperationException {
        byte[] v1Val = "ver1val".getBytes(METADATA_CHARSET);
        byte[] v2Val = "ver2val".getBytes(METADATA_CHARSET);
        byte[] v4Val = "ver1val".getBytes(METADATA_CHARSET);
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
    public void testDatabaseScan() throws RocksDBException, DatabaseOperationException {
        byte[][] keys = new byte[6][];
        byte[][] vals = new byte[6][];
        for (int i = 0; i < 6; i++) {
            keys[i] = KeyEncodingUtil.constructDatabaseKey(key1,i);
            vals[i] = ("ver" + i + "val").getBytes(METADATA_CHARSET);
        }
        try {
            databaseHandler.addTable(stream1);
            for (int i = 0; i < 6; i++) {
                databaseHandler.update(keys[i], vals[i], stream1);
                List<byte[][]> entries = databaseHandler.scan(stream1, 0);
                assertTrue(entries.size() == 1);
                assertArrayEquals(keys[i], entries.get(0)[0]);
                assertArrayEquals(vals[i], entries.get(0)[1]);
            }
        } catch (RocksDBException | DatabaseOperationException e) {
            log.error("Error in test SCAN: ", e);
            throw e;
        }
    }
}
