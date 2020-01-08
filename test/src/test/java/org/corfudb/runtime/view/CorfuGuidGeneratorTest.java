package org.corfudb.runtime.view;

import org.junit.Test;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validate that Corfu token based Guid Generator satisfies the following two criteria:
 * 1. The guids are unique.
 * 2. The guids are monotonically increasing.
 * Created by Sundar Sridharan on May 23, 2019.
 */
public class CorfuGuidGeneratorTest extends AbstractViewTest {

    @Test
    public void areUniqueAndOrderedLong() {
        final int iterations = PARAMETERS.NUM_ITERATIONS_MODERATE;
        CorfuGuidGenerator guidGenerator = CorfuGuidGenerator.getInstance(getDefaultRuntime().connect());
        long lastValue = 0;
        HashSet<Long> uniq = new HashSet<>(iterations);
        for (int i = 0; i < iterations; i++) {
            long current = guidGenerator.nextLong();
            CorfuGuid ts = new CorfuGuid(current);
            // Each time validate that number of transactions made is kept minimal.
            assertThat(ts.getUniqueInstanceId()).isLessThan(PARAMETERS.NUM_ITERATIONS_LOW);
            assertThat(guidGenerator.toString(current)).isNotNull();
            // Validate uniqueness
            assertThat(uniq).doesNotContain(current);
            // Validate monotonic increasing order
            assertThat(current).isGreaterThan(lastValue);
            lastValue = current;
            uniq.add(current);
        }
    }
}

