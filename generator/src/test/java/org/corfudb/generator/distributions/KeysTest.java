package org.corfudb.generator.distributions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KeysTest {

    @Test
    public void test() {
        Keys keys = new Keys(1);
        keys.populate();
        assertEquals("0", keys.sample().getKey());
    }
}