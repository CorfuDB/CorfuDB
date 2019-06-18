package org.corfudb.runtime.view;

import org.junit.Test;

import java.util.HashSet;
import java.util.UUID;

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
        OrderedGuidGenerator guidGenerator = new CorfuGuidGenerator(getDefaultRuntime().connect());
        long lastValue = 0;
        HashSet<Long> uniq = new HashSet<>(iterations);
        for (int i = 0; i < iterations; i++) {
            long current = guidGenerator.nextLong();
            assertThat(uniq).doesNotContain(current);
            assertThat(current).isGreaterThan(lastValue);
            lastValue = current;
            uniq.add(current);
        }
    }

    @Test
    public void areUniqueAndOrderedUUID() {
        final int iterations = PARAMETERS.NUM_ITERATIONS_MODERATE;
        OrderedGuidGenerator guidGenerator = new CorfuGuidGenerator(getDefaultRuntime().connect());
        UUID lastValue = new UUID(0,0);
        HashSet<UUID> uniq = new HashSet<>(iterations);
        for (int i = 0; i < iterations; i++) {
            UUID current = guidGenerator.nextUUID();
            assertThat(uniq).doesNotContain(current);
            assertThat(current).isGreaterThan(lastValue);
            lastValue = current;
            uniq.add(current);
        }
    }
}

