package org.corfudb.infrastructure.log;

import static org.junit.Assert.*;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.runtime.view.Address;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class StreamLogDataStoreTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testGetAndSave() {
        StreamLogDataStore streamLogDs = getStreamLogDataStore();

        final long headSegment = 555L;
        streamLogDs.updateHeadSegment(headSegment);
        assertEquals(headSegment, streamLogDs.getHeadSegment());

        final long tailSegment = 333L;
        streamLogDs.updateTailSegment(tailSegment);
        assertEquals(tailSegment, streamLogDs.getTailSegment());

        final long compactionMark = 666L;
        streamLogDs.updateGlobalCompactionMark(compactionMark);
        assertEquals(compactionMark, streamLogDs.getGlobalCompactionMark());

        final long committedTail = 777L;
        streamLogDs.updateCommittedTail(committedTail);
        assertEquals(committedTail, streamLogDs.getCommittedTail());
    }

    @Test
    public void testReset() {
        StreamLogDataStore streamLogDs = getStreamLogDataStore();

        final long initialValue = 100L;

        streamLogDs.updateHeadSegment(initialValue);
        streamLogDs.updateTailSegment(initialValue);
        streamLogDs.updateGlobalCompactionMark(initialValue);
        streamLogDs.updateCommittedTail(initialValue);

        streamLogDs.resetHeadSegment();
        assertEquals(streamLogDs.getHeadSegment(), Address.MAX);

        streamLogDs.resetTailSegment();
        assertEquals(streamLogDs.getTailSegment(), Address.getMinAddress());

        streamLogDs.resetGlobalCompactionMark();
        assertEquals(streamLogDs.getGlobalCompactionMark(), Address.NON_ADDRESS);

        streamLogDs.resetCommittedTail();
        assertEquals(streamLogDs.getCommittedTail(), Address.NON_ADDRESS);
    }

    private StreamLogDataStore getStreamLogDataStore() {
        Map<String, Object> opts = new HashMap<>();
        opts.put("--log-path", tempDir.getRoot().getAbsolutePath());

        DataStore ds = new DataStore(opts, val -> log.info("clean up"));

        return new StreamLogDataStore(ds);
    }
}
