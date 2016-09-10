package org.corfudb.runtime.collections;

import org.corfudb.runtime.view.Layout;
import org.junit.Before;

import java.util.Collections;

/**
 * Created by mwei on 1/8/16.
 */
public class ReplexSMRMapTest extends SMRMapTest {

    @Before
    @Override
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
        // First commit a layout that uses Replex
        Layout newLayout = r.layout.get();
        newLayout.getSegment(0L).setReplicationMode(Layout.ReplicationMode.REPLEX);
        newLayout.getSegment(0L).setReplexes(Collections.singletonList(
                new Layout.LayoutStripe(Collections.singletonList(defaultConfigurationString))));
        r.getLayoutView().committed(0L, newLayout);
        r.invalidateLayout();
    }
}
