package org.corfudb.protocols.wireprotocol;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TokenTest {

    @Test
    public void tokenComparisionTest() {

        Token uninitialized = Token.UNINITIALIZED;
        Token a = new Token(0, 0);
        Token a2 = new Token(0, 0);
        Token b = new Token(0, 1);

        // Verify that uninitialized < a <= a2 < b < c
        assertThat(uninitialized.compareTo(a)).isEqualTo(-1);
        assertThat(a.compareTo(a2)).isEqualTo(0);
        assertThat(a2.compareTo(b)).isEqualTo(-1);
        assertThat(b.compareTo(a)).isEqualTo(1);
    }
}
