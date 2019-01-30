package org.corfudb.runtime.view;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import java.util.TreeSet;

public class LayoutTest {

    @Test
    public void testLayoutComparator(){
        Layout l1 = mock(Layout.class);
        Layout l2 = mock(Layout.class);

        when(l1.getEpoch()).thenReturn(1L);
        when(l2.getEpoch()).thenReturn(2L);

        TreeSet<Layout> descendingOrder = new TreeSet<>(Layout.LAYOUT_COMPARATOR);
        descendingOrder.add(l1);
        descendingOrder.add(l2);

        assertEquals(l2.getEpoch(), descendingOrder.first().getEpoch());
    }

}