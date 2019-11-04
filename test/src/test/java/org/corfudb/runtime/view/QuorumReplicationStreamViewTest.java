package org.corfudb.runtime.view;

import org.corfudb.infrastructure.ServerContextBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

import java.util.Collections;

/**
 * Created by kspirov
 */
public class QuorumReplicationStreamViewTest extends StreamViewTest {
    @Before
    @Override
    public void setRuntime() throws Exception {
        ServerContextBuilder serverContextBuilder = new ServerContextBuilder()
                .setMemory(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .setCompactionPolicyType("GARBAGE_SIZE_FIRST")
                .setSegmentGarbageRatioThreshold("0")
                .setSegmentGarbageSizeThresholdMB("0");

        r = getDefaultRuntime(serverContextBuilder).connect();
        // First commit a layout that uses Quorum Replication
        Layout newLayout = r.getLayoutView().getLayout();
        newLayout.getSegment(0L).setReplicationMode(Layout.ReplicationMode.QUORUM_REPLICATION);
        newLayout.setEpoch(1);
        r.setCacheDisabled(true);
        r.getLayoutView().committed(1L, newLayout);
        r.invalidateLayout();
        r.getLayoutView().getRuntimeLayout(newLayout).getPrimarySequencerClient()
                .bootstrap(0L, Collections.emptyMap(), 1L, false).get();
    }


    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStream()
            throws Exception {
        super.canReadWriteFromStream();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStreamConcurrent()
            throws Exception {
        super.canReadWriteFromStreamConcurrent();
        assertNotNull(r.getAddressSpaceView().read(0L).getRank());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromCachedStream()
            throws Exception {
        super.canReadWriteFromCachedStream();
    }

    @Test
    public void canSeekOnStream()
            throws Exception
    {
        super.canSeekOnStream();
    }

    @Test
    public void canFindInStream()
            throws Exception
    {
        super.canFindInStream();
    }

    @Test
    public void canDoPreviousOnStream()
            throws Exception
    {
        super.canDoPreviousOnStream();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void streamCanSurviveOverwriteException()
            throws Exception {
        super.streamCanSurviveOverwriteException();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void streamWillHoleFill()
            throws Exception {
        super.streamWillHoleFill();
    }


    @Test
    @SuppressWarnings("unchecked")
    public void streamWithHoleFill()
            throws Exception {
        super.streamWithHoleFill();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCompactionMark() {
        super.testCompactionMark();
    }

}
