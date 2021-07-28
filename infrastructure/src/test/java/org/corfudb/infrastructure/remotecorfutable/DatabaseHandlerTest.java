package org.corfudb.infrastructure.remotecorfutable;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Rule;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.rocksdb.Options;

import java.nio.file.Path;
import java.util.concurrent.ThreadPoolExecutor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class DatabaseHandlerTest {
    @Rule
    public MockitoRule mockito = MockitoJUnit.rule();

    //Object to test
    private DatabaseHandler databaseHandler;

    //Objects to mock
    private Path mPath;
    private Options mOptions;
    private ThreadPoolExecutor mThreadPoolExecutor;

    @Before
    public void setup() {
        mPath = mock(Path.class);
        mOptions = mock(Options.class);
        mThreadPoolExecutor = mock(ThreadPoolExecutor.class);

        String TEST_TEMP_DIR = com.google.common.io.Files.createTempDir().getAbsolutePath();

        when(mPath.toFile().getAbsolutePath()).thenReturn(TEST_TEMP_DIR);
    }

}
