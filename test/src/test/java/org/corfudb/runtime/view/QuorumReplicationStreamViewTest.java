package org.corfudb.runtime.view;

import org.junit.Before;
import org.junit.Ignore;

import java.util.Collections;

/**
 * Created by kspirov
 */
public class QuorumReplicationStreamViewTest extends StreamViewTest {
    @Before
    @Override
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
        // First commit a layout that uses Replex
        Layout newLayout = r.layout.get();
        newLayout.getSegment(0L).setReplicationMode(Layout.ReplicationMode.QUORUM_REPLICATION);
        newLayout.setEpoch(1);
        r.getLayoutView().committed(1L, newLayout);
        r.invalidateLayout();
    }

}
