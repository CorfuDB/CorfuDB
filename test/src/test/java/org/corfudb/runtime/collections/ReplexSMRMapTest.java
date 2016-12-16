package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.test.DisabledOnTravis;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/8/16.
 */
@DisabledOnTravis
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
        newLayout.setEpoch(1);
        r.getLayoutView().committed(1, newLayout);
        r.invalidateLayout();
    }
}
