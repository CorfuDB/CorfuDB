package org.corfudb.protocols.wireprotocol;

import org.junit.Test;

import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LayoutPrepareResponseTest {

    @Test
    public void testLayoutPrepareResponseComparator(){
        LayoutPrepareResponse l1 = mock(LayoutPrepareResponse.class);
        LayoutPrepareResponse l2 = mock(LayoutPrepareResponse.class);

        when(l1.getRank()).thenReturn(1L);
        when(l2.getRank()).thenReturn(2L);

        TreeSet<LayoutPrepareResponse> descendingOrder =
                new TreeSet<>(LayoutPrepareResponse.LAYOUT_PREPARE_RESPONSE_COMPARATOR);
        descendingOrder.add(l1);
        descendingOrder.add(l2);

        assertEquals(l2.getRank(), descendingOrder.first().getRank());
    }
}