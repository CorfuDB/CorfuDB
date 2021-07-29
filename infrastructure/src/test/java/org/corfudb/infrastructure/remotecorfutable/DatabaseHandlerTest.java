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
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ThreadPoolExecutor;

import static org.corfudb.infrastructure.remotecorfutable.utils.DatabaseConstants.METADATA_CHARSET;
import static org.junit.Assert.assertEquals;
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
        byte[] expectedValue = "ver0val".getBytes(StandardCharsets.UTF_8);
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
        assertTrue(Arrays.equals(expectedValue,readinVal));
    }

}
