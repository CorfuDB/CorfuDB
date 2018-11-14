package org.corfudb.protocols.wireprotocol;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LSNTest {

    @Test
    public void tokenComparisionTest() {

        LSN uninitialized = LSN.UNINITIALIZED;
        LSN a = new LSN(0, 0);
        LSN a2 = new LSN(0, 0);
        LSN b = new LSN(0, 1);

        // Verify that uninitialized < a <= a2 < b < c
        assertThat(uninitialized.compareTo(a)).isEqualTo(-1);
        assertThat(a.compareTo(a2)).isEqualTo(0);
        assertThat(a2.compareTo(b)).isEqualTo(-1);
        assertThat(b.compareTo(a)).isEqualTo(1);
    }
}
