package org.corfudb.generator.distributions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationCountTest {

    @Test
    void testSample() {
        OperationCount count = new OperationCount();
        Integer sample = count.sample();
        assertTrue(sample > 0 && sample < 101);
    }
}