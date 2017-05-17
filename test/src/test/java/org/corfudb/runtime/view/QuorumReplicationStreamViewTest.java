package org.corfudb.runtime.view;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Created by kspirov
 */
public class QuorumReplicationStreamViewTest extends StreamViewTest {
    @Before
    @Override
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
        // First commit a layout that uses Replex
        Layout newLayout = r.layoutFuture.get();
        newLayout.getSegment(0L).setReplicationMode(Layout.ReplicationMode.QUORUM_REPLICATION);
        newLayout.setEpoch(1);
        r.setCacheDisabled(true);
        r.getLayoutView().committed(1L, newLayout);
        r.invalidateLayout();
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
    public void canReadWriteFromStreamWithoutBackpointers()
            throws Exception {
        super.canReadWriteFromStreamWithoutBackpointers();
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

}
