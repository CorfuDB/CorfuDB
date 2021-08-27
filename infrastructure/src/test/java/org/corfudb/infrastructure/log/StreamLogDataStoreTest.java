package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.configuration.ServerConfiguration;
import org.corfudb.infrastructure.datastore.DataStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

@Slf4j
public class StreamLogDataStoreTest {
    private static final long ZERO_ADDRESS = 0L;
    private static final long NON_ADDRESS = -1;

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testGetAndSave() {
        StreamLogDataStore streamLogDs = getStreamLogDataStore();
        final long tailSegment = 333;
        final long startingAddress = 444;
        final long committedTail = 555;

        streamLogDs.updateTailSegment(tailSegment);
        streamLogDs.updateStartingAddress(startingAddress);
        streamLogDs.updateCommittedTail(committedTail);

        assertEquals(tailSegment, streamLogDs.getTailSegment());
        assertEquals(startingAddress, streamLogDs.getStartingAddress());
        assertEquals(committedTail, streamLogDs.getCommittedTail());

        // TailSegment should be monotonic.
        streamLogDs.updateTailSegment(tailSegment - 1);
        assertEquals(tailSegment, streamLogDs.getTailSegment());
        streamLogDs.updateTailSegment(tailSegment + 1);
        assertEquals(tailSegment + 1, streamLogDs.getTailSegment());

        // StartingAddress should be monotonic.
        streamLogDs.updateStartingAddress(startingAddress - 1);
        assertEquals(startingAddress, streamLogDs.getStartingAddress());
        streamLogDs.updateStartingAddress(startingAddress + 1);
        assertEquals(startingAddress + 1, streamLogDs.getStartingAddress());

        // CommittedTail should be monotonic.
        streamLogDs.updateCommittedTail(committedTail - 1);
        assertEquals(committedTail, streamLogDs.getCommittedTail());
        streamLogDs.updateCommittedTail(committedTail + 1);
        assertEquals(committedTail + 1, streamLogDs.getCommittedTail());
    }

    @Test
    public void testReset() {
        StreamLogDataStore streamLogDs = getStreamLogDataStore();
        streamLogDs.resetStartingAddress();
        assertEquals(ZERO_ADDRESS, streamLogDs.getStartingAddress());

        streamLogDs.resetTailSegment();
        assertEquals(ZERO_ADDRESS, streamLogDs.getTailSegment());

        streamLogDs.resetCommittedTail();
        assertEquals(NON_ADDRESS, streamLogDs.getCommittedTail());
    }

    private StreamLogDataStore getStreamLogDataStore() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setServerDirectory(tempDir.getRoot().getAbsolutePath());

        DataStore ds = new DataStore(conf, val -> log.info("clean up"));

        return new StreamLogDataStore(ds);
    }
}
