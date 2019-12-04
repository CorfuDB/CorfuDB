package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.configuration.ServerConfiguration;
import org.corfudb.infrastructure.datastore.DataStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Slf4j
public class StreamLogDataStoreTest {
    private static final long INITIAL_ADDRESS = 0L;

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testGetAndSave() {
        StreamLogDataStore streamLogDs = getStreamLogDataStore();

        final int tailSegment = 333;
        streamLogDs.updateTailSegment(tailSegment);
        assertEquals(tailSegment, streamLogDs.getTailSegment());

        final int startingAddress = 555;
        streamLogDs.updateStartingAddress(startingAddress);
        assertEquals(startingAddress, streamLogDs.getStartingAddress());
    }

    @Test
    public void testReset() {
        StreamLogDataStore streamLogDs = getStreamLogDataStore();
        streamLogDs.resetStartingAddress();
        assertEquals(INITIAL_ADDRESS, streamLogDs.getStartingAddress());

        streamLogDs.resetTailSegment();
        assertEquals(INITIAL_ADDRESS, streamLogDs.getTailSegment());
    }

    private StreamLogDataStore getStreamLogDataStore() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setServerDirectory(tempDir.getRoot().getAbsolutePath());

        DataStore ds = new DataStore(conf, val -> log.info("clean up"));

        return StreamLogDataStore.builder()
                .dataStore(ds)
                .build();
    }
}
